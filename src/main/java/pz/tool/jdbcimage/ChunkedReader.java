package pz.tool.jdbcimage;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of a reader that operates over chunks of char[]. 
 * @author zavora
 */
public class ChunkedReader extends Reader{
	private static final char[] EMPTY_CHUNK = new char[0];
	private Iterator<char[]> chunks;
	private long totalLength;
	private boolean finished;
	private char[] currentChunk;
	private int pos;
	
	public ChunkedReader(char[] chunk){
		this(Arrays.asList(chunk), chunk.length);
	}

	public ChunkedReader(List<char[]> chunks, long totalLength){
		if (chunks == null || chunks.isEmpty()){
			finished = true;
		} else{
			if (totalLength <= 0){
				// calculate length
				totalLength = chunks.stream().mapToLong(x -> x==null?0:x.length).sum();
			}
			this.totalLength = totalLength;
			this.chunks = chunks.iterator();
			this.currentChunk = this.chunks.next();
			this.pos = 0;
			if (currentChunk == null) currentChunk = EMPTY_CHUNK;
			forward(0); // skip possibly empty chunks
		}
	}
	
	public long length(){
		return totalLength;
	}
	
	/**
	 * Moves the internal pointer.
	 * @param count count of position to move
	 * @return false if an end of stream was reached  
	 */
	private boolean forward(int count){
		while(count>=(currentChunk.length-pos)){
			count-=currentChunk.length-pos;
			pos = 0;
			if (chunks.hasNext()){
				currentChunk = chunks.next();
				if (currentChunk == null) currentChunk = EMPTY_CHUNK;
			} else{
				finished = true;
				return false;
			}
		}
		pos+= count;
		return true;
	}
	
	@Override
	public int read() throws IOException {
		if (finished) return -1;
		int retVal = currentChunk[pos];
		forward(1);
		return retVal;
	}

	@Override
	public int read(char[] b, int off, int len) throws IOException {
		if (finished) return -1;
		if (len <= 0) return 0;
		int remaining = len;
		while(remaining>=(currentChunk.length-pos)){
			System.arraycopy(currentChunk, pos, b, off+len-remaining, currentChunk.length-pos);
			remaining -= currentChunk.length-pos;
			if (!forward(currentChunk.length-pos)){
				return len-remaining;
			}
		}
		if (remaining>0){
			System.arraycopy(currentChunk, pos, b, off+len-remaining, remaining);
			forward(remaining);
			remaining = 0;
		}
		return len-remaining;
	}
	
	public String readAsString(){
		if (chunks == null){
			return "";
		} else{
			StringBuffer buffer = new StringBuffer((int)totalLength);
			while(chunks.hasNext()){
				char[] next = chunks.next();
				buffer.append(next);
			}
			return buffer.toString();
		}
	}

	@Override
	public void close() throws IOException {
		// clean up
		chunks = null;
	}
}
