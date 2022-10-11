package io.github.sranka.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.github.sranka.jdbcimage.ChunkedReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Stream (de)serializer with optimized method for CLOB handling 
 * @author zavora
 */
public class KryoReaderSerializer extends Serializer<Reader>{
	private static final Log log = LogFactory.getLog(KryoReaderSerializer.class);
	public static KryoReaderSerializer INSTANCE = new KryoReaderSerializer();
	private static final int BUFFER_SIZE = 1024 * 32;
	
	/**
	 * Helper class to create clobs.
	 */
	private static class ClobSupplier{
		
		public Clob createClob(Connection con) throws SQLException{
			return con.createClob();
		}
	}
	private static class NClobSupplier extends ClobSupplier{
		public Clob createClob(Connection con) throws SQLException{
			return con.createNClob();
		}
	}
	private static ClobSupplier CLOB_SUPPLIER = new ClobSupplier();
	private static ClobSupplier NCLOB_SUPPLIER = new NClobSupplier();

	public Object deserializeClobData(Input in, Connection connection){
		return deserializeClobData(in, connection, CLOB_SUPPLIER);
	}
	public Object deserializeNClobData(Input in, Connection connection){
		return deserializeClobData(in, connection, NCLOB_SUPPLIER);
	}
	private Object deserializeClobData(Input in, Connection connection, ClobSupplier clobSupplier){
		// read one byte to know if there is a stream
		if (in.readByte() == Kryo.NULL){ 
			return null;
		}

		long total = 0;
		int count;
		
		// read a first chunk 
		count = in.readInt(); // not null->first chunk is always available
		if (count == -1){
			return new ChunkedReader(new char[0]);
		}
		char[] firstChars = in.readChars(count);
		total += count;

		// read next chunks
		ArrayList<char[]> chunks = null;
		while ((count = in.readInt())!=-1){
			total+=count;
			// create BLOB or input stream
			if (connection == null){
				// input stream
				if (chunks == null){
					chunks = new ArrayList<>();
					chunks.add(firstChars);
				}
				chunks.add(in.readChars(count));
			} else{
				// blob
				try{
					if (log.isDebugEnabled()) log.debug("Creating database clob");
					Clob clob = clobSupplier.createClob(connection);
					Writer out = clob.setCharacterStream(1);
					out.write(firstChars);// print out first chunk
					char[] buffer = firstChars.length<(in.getBuffer().length*2)?new char[in.getBuffer().length]:firstChars;
					transferToWriter(count, buffer, in, out);
					return clob;
				} catch (SQLException | IOException e) {
					throw new RuntimeException(e); 
				}
			}
		}
		
		if (chunks!=null){
			return new ChunkedReader(chunks, total);
		} else{
			return new ChunkedReader(firstChars);
		}
	}
	/**
	 * Called to transfer `count` characters from the buffer and then 
	 * reuse the buffer to copy the whole data stream.
	 * @param count initial count to copy from input, non-negative
	 * @param buffer buffer to use
	 * @param in input to read from
	 * @param out blob to write to
	 * @throws IOException writer error
	 */
	private void transferToWriter(int count, char[] buffer, Input in, Writer out) throws IOException{
		do{
			// read count using a buffer
			while (count>0){
				int toReadCount = Math.min(count, buffer.length);
				for(int j=0; j<toReadCount; j++){
					buffer[j] = in.readChar();
				}
				out.write(buffer,0,toReadCount);
				count-=toReadCount;
			}
		}while((count = in.readInt())!=-1);
		out.flush(); // no more data to write
	}
	
	@Override
	public Reader read(Kryo kryo, Input in, Class<Reader> type) {
		ArrayList<char[]> chunks = new ArrayList<>(); 
		// not supported
		long total = 0;
		int count;
		while((count = in.readInt())!=-1){
			total+=count;
			chunks.add(in.readChars(count));
		}
		return new ChunkedReader(chunks, total);
	}

	@Override
	public void write(Kryo kryo, Output out, Reader in) {
		try {
			// write chunks until EOF is found
			char[] buffer = new char[BUFFER_SIZE];
			int count;
			int chunks = 0;
			while((count = in.read(buffer))!=-1){
				if (count == 0) continue; // just in case, robust
				out.writeInt(count);
				chunks++;
				for(int j=0; j<count; j++){
					out.writeChar(buffer[j]);
				}
			}
			chunkInfo(chunks);
			out.writeInt(-1);// tail marker
			in.close(); // close the input stream
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	protected void chunkInfo(int chunks){
		if (chunks>1 && log.isDebugEnabled()) log.debug(" --> chunks:"+chunks);
	}
}
