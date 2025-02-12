package io.github.sranka.jdbcimage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of an input stream that operates over chunks by byte[].
 */
@SuppressWarnings("DuplicatedCode")
public class ChunkedInputStream extends InputStream {
    private static final byte[] EMPTY_CHUNK = new byte[0];
    private Iterator<byte[]> chunks;
    private long totalLength;
    private boolean finished;
    private byte[] currentChunk;
    private int pos;

    public ChunkedInputStream(List<byte[]> chunks, long totalLength) {
        if (chunks == null || chunks.isEmpty()) {
            finished = true;
        } else {
            if (totalLength <= 0) {
                // calculate length
                totalLength = chunks.stream().mapToLong(x -> x != null ? x.length : 0).sum();
            }
            this.totalLength = totalLength;
            this.chunks = chunks.iterator();
            this.currentChunk = this.chunks.next();
            this.pos = 0;
            if (currentChunk == null) currentChunk = EMPTY_CHUNK;
            forward(0); // skip possibly empty chunks
        }
    }

    public long length() {
        return totalLength;
    }

    /**
     * Moves the internal pointer.
     *
     * @param count count of position to move
     * @return false if an end of stream was reached
     */
    private boolean forward(int count) {
        while (count >= (currentChunk.length - pos)) {
            count -= currentChunk.length - pos;
            pos = 0;
            if (chunks.hasNext()) {
                currentChunk = chunks.next();
                if (currentChunk == null) currentChunk = EMPTY_CHUNK;
            } else {
                finished = true;
                return false;
            }
        }
        pos += count;
        return true;
    }

    @Override
    public int read() throws IOException {
        if (finished) return -1;
        int retVal = currentChunk[pos] & 0xFF;
        forward(1);
        return retVal;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (finished) return -1;
        if (len <= 0) return 0;
        int remaining = len;
        while (remaining >= (currentChunk.length - pos)) {
            System.arraycopy(currentChunk, pos, b, off + len - remaining, currentChunk.length - pos);
            remaining -= currentChunk.length - pos;
            if (!forward(currentChunk.length - pos)) {
                return len - remaining;
            }
        }
        if (remaining > 0) {
            System.arraycopy(currentChunk, pos, b, off + len - remaining, remaining);
            forward(remaining);
            remaining = 0;
        }
        return len - remaining;
    }
}
