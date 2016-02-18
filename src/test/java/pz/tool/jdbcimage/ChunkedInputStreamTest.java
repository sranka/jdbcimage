package pz.tool.jdbcimage;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

/**
 * Unit test of associated class.
 */
public class ChunkedInputStreamTest {
    @Test
    public void testEmptyStreamSingle() throws IOException {
        ChunkedInputStream cis1 = new ChunkedInputStream(null, 0);
        assertTrue(cis1.read()==-1);
        assertTrue(cis1.read()==-1); // repetitive read is no problem
        ChunkedInputStream cis2 = new ChunkedInputStream(Collections.emptyList(), -1);
        assertTrue(cis2.read()==-1);
        assertTrue(cis2.read()==-1); // repetitive read is no problem
        ChunkedInputStream cis3 = new ChunkedInputStream(Arrays.asList(null,new byte[0]), -1);
        assertTrue(cis3.read()==-1);
        assertTrue(cis3.read()==-1); // repetitive read is no problem
    }
    @Test
    public void testEmptyStreamBuffer() throws IOException {
        byte[] buffer = new byte[10];
        try(ChunkedInputStream cis1 = new ChunkedInputStream(null, 0)) {
            assertTrue(cis1.read(buffer, 0, 10) == -1);
            assertTrue(cis1.read(buffer, 0, 10) == -1); // repetitive read is no problem
        }
        try(ChunkedInputStream cis2 = new ChunkedInputStream(Collections.emptyList(), -1)) {
            assertTrue(cis2.read(buffer, 0, 10) == -1);
            assertTrue(cis2.read(buffer, 0, 10) == -1); // repetitive read is no problem
        }
        try(ChunkedInputStream cis3 = new ChunkedInputStream(Arrays.asList(null,new byte[0]), -1)) {
            assertTrue(cis3.read(buffer, 0, 10) == -1);
            assertTrue(cis3.read(buffer, 0, 10) == -1); // repetitive read is no problem
        }
    }
    @Test
    public void testStreamOfUnknownSize(){
        ChunkedInputStream cis1 = new ChunkedInputStream(null, 0);
        assertTrue(cis1.length()==0);
        ChunkedInputStream cis2 = new ChunkedInputStream(Collections.emptyList(), -1);
        assertTrue(cis2.length()==0);
        ChunkedInputStream cis3 = new ChunkedInputStream(Arrays.asList(null,new byte[0]), -1);
        assertTrue(cis3.length()==0);
        ChunkedInputStream cis4 = new ChunkedInputStream(Arrays.asList(new byte[2]), -1);
        assertTrue(cis4.length()==2);
        ChunkedInputStream cis5 = new ChunkedInputStream(Arrays.asList(new byte[2], new byte[3], new byte[4]), -1);
        assertTrue(cis5.length()==9);
    }
    @Test
    public void testOneChunkStreamSingle() throws IOException {
        byte[] chunk = new byte[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (byte)(255-i);
        ChunkedInputStream cis = new ChunkedInputStream(Arrays.asList(chunk), -1);
        assertTrue(cis.length()==chunk.length);
        for (byte aChunk : chunk) {
            int val = cis.read();
            assertTrue(val >= 0);
            assertTrue(val == (aChunk & 0xFF));
        }
        assertTrue(cis.read() == -1);
    }
    @Test
    public void testOneChunkStreamBuffer() throws IOException {
        byte[] chunk = new byte[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (byte)(255-i);
        ChunkedInputStream cis = new ChunkedInputStream(Arrays.asList(chunk), -1);
        assertTrue(cis.length()==chunk.length);
        byte[] buffer = new byte[4];
        int total = 0;
        int val;
        for(;;){
            val = cis.read(buffer,1,2);
            if (val<=0){
                break;
            }
            assertTrue(val<=2);
            assertTrue(buffer[1] == chunk[total++]);
            if (val>1) assertTrue(buffer[2] == chunk[total++]);
        }
        assertTrue(val == -1);
        assertTrue(total == chunk.length);
        assertTrue(total == cis.length());
        assertTrue(cis.read(buffer,1,2) == -1);
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
        ChunkedInputStream cis = new ChunkedInputStream(chunks, -1);
        assertTrue(cis.length()==size);
        int total = 0;
        while(total<size){
            int val = cis.read();
            assertTrue(val>=0);
            assertTrue(val == (total&0xFF));
            total++;
        }
        assertTrue(cis.read() == -1);
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
        ChunkedInputStream cis = new ChunkedInputStream(chunks, -1);
        assertTrue(cis.length()==size);

        byte[] buffer = new byte[4];
        int total = 0;
        int val;
        for(;;){
            val = cis.read(buffer,1,2);
            if (val<=0){
                break;
            }
            assertTrue(val<=2);
            assertTrue((buffer[1]&0xFF) == (total&0xFF));
            total++;
            if (val>1) {
                assertTrue((buffer[2]&0xFF) == (total&0xFF));
                total++;
            }
        }
        assertTrue(val == -1);
        assertTrue(total == size);
        assertTrue(cis.read(buffer,1,2) == -1);

    }

}
