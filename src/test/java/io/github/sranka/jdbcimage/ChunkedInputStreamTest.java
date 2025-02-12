package io.github.sranka.jdbcimage;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test of associated class.
 */
public class ChunkedInputStreamTest {
    @Test
    public void testEmptyStreamSingle() throws IOException {
        try(ChunkedInputStream cis1 = new ChunkedInputStream(null, 0)) {
            assertEquals(-1, cis1.read());
            assertEquals(-1, cis1.read()); // repetitive read is no problem
        }
        try(ChunkedInputStream cis2 = new ChunkedInputStream(Collections.emptyList(), -1)){
            assertEquals(-1, cis2.read());
            assertEquals(-1, cis2.read()); // repetitive read is no problem
        }
        try(ChunkedInputStream cis3 = new ChunkedInputStream(Arrays.asList(null,new byte[0]), -1)){
            assertEquals(-1, cis3.read());
            assertEquals(-1, cis3.read()); // repetitive read is no problem
        }
    }
    @Test
    public void testEmptyStreamBuffer() throws IOException {
        byte[] buffer = new byte[10];
        try(ChunkedInputStream cis1 = new ChunkedInputStream(null, 0)) {
            assertEquals(-1, cis1.read(buffer, 0, 10));
            assertEquals(-1, cis1.read(buffer, 0, 10)); // repetitive read is no problem
        }
        try(ChunkedInputStream cis2 = new ChunkedInputStream(Collections.emptyList(), -1)) {
            assertEquals(-1, cis2.read(buffer, 0, 10));
            assertEquals(-1, cis2.read(buffer, 0, 10)); // repetitive read is no problem
        }
        try(ChunkedInputStream cis3 = new ChunkedInputStream(Arrays.asList(null,new byte[0]), -1)) {
            assertEquals(-1, cis3.read(buffer, 0, 10));
            assertEquals(-1, cis3.read(buffer, 0, 10)); // repetitive read is no problem
        }
    }
    @Test
    public void testStreamOfUnknownSize() throws IOException{
        try(ChunkedInputStream cis1 = new ChunkedInputStream(null, 0)) {
            assertEquals(0, cis1.length());
        }
        try(ChunkedInputStream cis2 = new ChunkedInputStream(Collections.emptyList(), -1)) {
            assertEquals(0, cis2.length());
        }
        try(ChunkedInputStream cis3 = new ChunkedInputStream(Arrays.asList(null,new byte[0]), -1)) {
            assertEquals(0, cis3.length());
        }
        try(ChunkedInputStream cis4 = new ChunkedInputStream(Collections.singletonList(new byte[2]), -1)) {
            assertEquals(2, cis4.length());
        }
        try(ChunkedInputStream cis5 = new ChunkedInputStream(Arrays.asList(new byte[2], new byte[3], new byte[4]), -1)) {
            assertEquals(9, cis5.length());
        }
    }
    @Test
    public void testOneChunkStreamSingle() throws IOException {
        byte[] chunk = new byte[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (byte)(255-i);
        try(ChunkedInputStream cis = new ChunkedInputStream(Collections.singletonList(chunk), -1)) {
            assertEquals(chunk.length, cis.length());
            for (byte aChunk : chunk) {
                int val = cis.read();
                assertTrue(val >= 0);
                assertEquals(val, (aChunk & 0xFF));
            }
            assertEquals(-1, cis.read());
        }
    }
    @Test
    public void testOneChunkStreamBuffer() throws IOException {
        byte[] chunk = new byte[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (byte)(255-i);
        try(ChunkedInputStream cis = new ChunkedInputStream(Collections.singletonList(chunk), -1)) {
            assertEquals(chunk.length, cis.length());
            byte[] buffer = new byte[4];
            int total = 0;
            int val;
            for (; ; ) {
                val = cis.read(buffer, 1, 2);
                if (val <= 0) {
                    break;
                }
                assertTrue(val <= 2);
                assertEquals(buffer[1], chunk[total++]);
                if (val > 1) assertEquals(buffer[2], chunk[total++]);
            }
            assertEquals(-1, val);
            assertEquals(chunk.length, total);
            assertEquals(-1, cis.read(buffer, 1, 2));
        }
    }
    @Test
    public void testMultipleChunkStreamSingle() throws IOException {
        ArrayList<byte[]> chunks = new ArrayList<>();
        int size = 0;
        for(int i=1; i<=40; i++){
            byte[] chunk = new byte[i];
            chunks.add(chunk);
            for(int j=0; j<chunk.length; j++){
                chunk[j] = (byte)size++;
            }
        }
        try(ChunkedInputStream cis = new ChunkedInputStream(chunks, -1)) {
            assertEquals(cis.length(), size);
            int total = 0;
            while (total < size) {
                int val = cis.read();
                assertTrue(val >= 0);
                assertEquals(val, (total & 0xFF));
                total++;
            }
            assertEquals(-1, cis.read());
        }
    }
    @Test
    public void testMultipleChunkStreamBuffer() throws IOException {
        ArrayList<byte[]> chunks = new ArrayList<>();
        int size = 0;
        for(int i=1; i<=40; i++){
            byte[] chunk = new byte[i];
            chunks.add(chunk);
            for(int j=0; j<chunk.length; j++){
                chunk[j] = (byte)size++;
            }
        }
        try(ChunkedInputStream cis = new ChunkedInputStream(chunks, -1)) {
            assertEquals(cis.length(), size);

            byte[] buffer = new byte[4];
            int total = 0;
            int val;
            for (; ; ) {
                val = cis.read(buffer, 1, 2);
                if (val <= 0) {
                    break;
                }
                assertTrue(val <= 2);
                assertEquals((buffer[1] & 0xFF), (total & 0xFF));
                total++;
                if (val > 1) {
                    assertEquals((buffer[2] & 0xFF), (total & 0xFF));
                    total++;
                }
            }
            assertEquals(-1, val);
            assertEquals(total, size);
            assertEquals(-1, cis.read(buffer, 1, 2));
        }
    }

}
