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
public class ChunkedReaderTest {
    @Test
    public void testEmptyStreamSingle() throws IOException {
        try(ChunkedReader cis1 = new ChunkedReader(null, 0)) {
            assertEquals(-1, cis1.read());
            assertEquals(-1, cis1.read()); // repetitive read is no problem
        }
        try(ChunkedReader cis2 = new ChunkedReader(Collections.emptyList(), -1)) {
            assertEquals(-1, cis2.read());
            assertEquals(-1, cis2.read()); // repetitive read is no problem
        }
        try(ChunkedReader cis3 = new ChunkedReader(Arrays.asList(null,new char[0]), -1)) {
            assertEquals(-1, cis3.read());
            assertEquals(-1, cis3.read()); // repetitive read is no problem
        }
    }
    @Test
    public void testEmptyStreamBuffer() throws IOException {
        char[] buffer = new char[10];
        try(ChunkedReader cis1 = new ChunkedReader(null, 0)){
            assertEquals(-1, cis1.read(buffer, 0, 10));
            assertEquals(-1, cis1.read(buffer, 0, 10)); // repetitive read is no problem
        }
        try(ChunkedReader cis2 = new ChunkedReader(Collections.emptyList(), -1)) {
            assertEquals(-1, cis2.read(buffer, 0, 10));
            assertEquals(-1, cis2.read(buffer, 0, 10)); // repetitive read is no problem
        }
        try(ChunkedReader cis3 = new ChunkedReader(Arrays.asList(null,new char[0]), -1)) {
            assertEquals(-1, cis3.read(buffer, 0, 10));
            assertEquals(-1, cis3.read(buffer, 0, 10)); // repetitive read is no problem
        }
    }
    @Test
    public void testStreamOfUnknownSize() throws IOException{
        try(ChunkedReader cis1 = new ChunkedReader(null, 0)) {
            assertEquals(0, cis1.length());
        }
        try(ChunkedReader cis2 = new ChunkedReader(Collections.emptyList(), -1)) {
            assertEquals(0,cis2.length());
        }
        try(ChunkedReader cis3 = new ChunkedReader(Arrays.asList(null,new char[0]), -1)) {
            assertEquals(0, cis3.length());
        }
        try(ChunkedReader cis4 = new ChunkedReader(Collections.singletonList(new char[2]), -1)) {
            assertEquals(2, cis4.length());
        }
        try(ChunkedReader cis5 = new ChunkedReader(Arrays.asList(new char[2], new char[3], new char[4]), -1)) {
            assertEquals(9, cis5.length());
        }
    }
    @Test
    public void testOneChunkStreamSingle() throws IOException {
        char[] chunk = new char[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (char)(255-i);
        try(ChunkedReader cis = new ChunkedReader(chunk)) {
            assertEquals(cis.length(), chunk.length);
            for (char aChunk : chunk) {
                int val = cis.read();
                assertTrue(val >= 0);
                assertEquals((aChunk & 0xFFFF), val);
            }
            assertEquals(-1, cis.read());
        }
    }
    @Test
    public void testOneChunkStreamBuffer() throws IOException {
        char[] chunk = new char[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (char)(255-i);
        try(ChunkedReader cis = new ChunkedReader(chunk)) {
            assertEquals(cis.length(), chunk.length);
            char[] buffer = new char[4];
            int total = 0;
            int val;
            for (; ; ) {
                val = cis.read(buffer, 1, 2);
                if (val <= 0) {
                    break;
                }
                assertTrue(val <= 2);
                assertEquals(chunk[total++], buffer[1]);
                if (val > 1) assertEquals(chunk[total++], buffer[2]);
            }
            assertEquals(-1, val);
            assertEquals(chunk.length, total);
            assertEquals(-1, cis.read(buffer, 1, 2));
        }
    }
    @Test
    public void testMultipleChunkStreamSingle() throws IOException {
        ArrayList<char[]> chunks = new ArrayList<>();
        int size = 0;
        for(int i=1; i<=40; i++){
            char[] chunk = new char[i];
            chunks.add(chunk);
            for(int j=0; j<chunk.length; j++){
                chunk[j] = (char)size++;
            }
        }
        try(ChunkedReader cis = new ChunkedReader(chunks, -1)) {
            assertEquals(cis.length(), size);
            int total = 0;
            while (total < size) {
                int val = cis.read();
                assertTrue(val >= 0);
                assertEquals((total & 0xFFFF), val);
                total++;
            }
            assertEquals(-1, cis.read());
        }
    }
    @Test
    public void testMultipleChunkStreamBuffer() throws IOException {
        ArrayList<char[]> chunks = new ArrayList<>();
        int size = 0;
        for(int i=1; i<=40; i++){
            char[] chunk = new char[i];
            chunks.add(chunk);
            for(int j=0; j<chunk.length; j++){
                chunk[j] = (char)size++;
            }
        }
        try(ChunkedReader cis = new ChunkedReader(chunks, -1)) {
            assertEquals(size, cis.length());

            char[] buffer = new char[4];
            int total = 0;
            int val;
            for (; ; ) {
                val = cis.read(buffer, 1, 2);
                if (val <= 0) {
                    break;
                }
                assertTrue(val <= 2);
                assertEquals((total & 0xFFFF), (buffer[1] & 0xFFFF));
                total++;
                if (val > 1) {
                    assertEquals((total & 0xFFFF), (buffer[2] & 0xFFFF));
                    total++;
                }
            }
            assertEquals(-1, val);
            assertEquals(size, total);
            assertEquals(-1, cis.read(buffer, 1, 2));
        }
    }

}
