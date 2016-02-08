package pz.tool.jdbcimage.kryo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import pz.tool.jdbcimage.ChunkedInputStream;

/**
 * Stream (de)serializer with optimized method for BLOB handling 
 * @author zavora
 */
public class KryoInputStreamSerializer extends Serializer<InputStream>{
	public static KryoInputStreamSerializer INSTANCE = new KryoInputStreamSerializer();
	private static final int BUFFER_SIZE = 1024 * 64;

	public Object deserializeBlobData(Input in, Connection connection){
		// read one byte to know if there is a stream
		if (in.readByte() == Kryo.NULL){ 
			return null;
		}

		long total = 0;
		int count;
		
		// read a first chunk 
		count = in.readInt(); // not null->first chunk is always available
		if (count == -1){
			return new byte[0]; // empty data
		}
		byte[] firstBytes = in.readBytes(count);
		total += count;

		// read next chunks
		ArrayList<byte[]> chunks = null;
		while ((count = in.readInt())!=-1){
			total+=count;
			// create BLOB or input stream
			if (connection == null){
				// input stream
				if (chunks == null){
					chunks = new ArrayList<byte[]>();
					chunks.add(firstBytes);
				}
				chunks.add(in.readBytes(count));
			} else{
				// blob
				try{
					// TODO verbose log creating Blob
					Blob blob = connection.createBlob();
					OutputStream out = blob.setBinaryStream(1);
					out.write(firstBytes);// print out first chunk
					byte[] buffer = firstBytes.length<in.getBuffer().length?new byte[in.getBuffer().length]:firstBytes;
					transferToOutputStream(count, buffer, in, out);
					return blob;
				} catch (SQLException | IOException e) {
					// TODO log
					throw new RuntimeException(e);
				}
			}
		}
		
		if (chunks!=null){
			return new ChunkedInputStream(chunks, total);
		} else{
			return firstBytes;
		}
	}
	/**
	 * Called to transfer `count` bytes from the buffer and then 
	 * reuse the buffer to copy the whole data stream.
	 * @param count initial count to copy from input, non-negative
	 * @param buffer buffer to use
	 * @param in input to read from
	 * @param out blob to write to
	 * @throws IOException
	 */
	private void transferToOutputStream(int count, byte[] buffer, Input in, OutputStream out) throws IOException{
		do{
			// read count using a buffer
			while (count>0){
				int toReadCount = count>buffer.length?buffer.length:count;
				in.readBytes(buffer, 0, toReadCount);
				out.write(buffer,0,toReadCount);
				count-=toReadCount;
			}
		}while((count = in.readInt())!=-1);
		out.flush(); // no more data to write
	}
	
	@Override
	public InputStream read(Kryo kryo, Input in, Class<InputStream> type) {
		ArrayList<byte[]> chunks = new ArrayList<>(); 
		// not supported
		long total = 0;
		int count;
		while((count = in.readInt())!=-1){
			total+=count;
			chunks.add(in.readBytes(count));
		}
		return new ChunkedInputStream(chunks, total);
	}

	@Override
	public void write(Kryo kryo, Output out, InputStream in) {
		try {
			// write chunks until EOF is found
			byte[] buffer = new byte[BUFFER_SIZE];
			int count;
			int chunks = 0;
			while((count = in.read(buffer))!=-1){
				if (count == 0) continue; // just in case, robust
				out.writeInt(count);
				chunks++;
				out.writeBytes(buffer,0,count);
			}
			chunkInfo(chunks);
			out.writeInt(-1);// tail marker
			in.close(); // close the input stream
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	protected void chunkInfo(int chunks){
		// TODO verbose log following
		// if (chunks>1) System.out.println(" --> chunks:"+chunks);
	}
}
