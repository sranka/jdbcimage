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
public class ChunkedReaderTest {
    @Test
    public void testEmptyStreamSingle() throws IOException {
        ChunkedReader cis1 = new ChunkedReader(null, 0);
        assertTrue(cis1.read()==-1);
        assertTrue(cis1.read()==-1); // repetitive read is no problem
        ChunkedReader cis2 = new ChunkedReader(Collections.emptyList(), -1);
        assertTrue(cis2.read()==-1);
        assertTrue(cis2.read()==-1); // repetitive read is no problem
        ChunkedReader cis3 = new ChunkedReader(Arrays.asList(null,new char[0]), -1);
        assertTrue(cis3.read()==-1);
        assertTrue(cis3.read()==-1); // repetitive read is no problem
    }
    @Test
    public void testEmptyStreamBuffer() throws IOException {
        char[] buffer = new char[10];
        try(ChunkedReader cis1 = new ChunkedReader(null, 0)){
            assertTrue(cis1.read(buffer,0,10)==-1);
            assertTrue(cis1.read(buffer,0,10)==-1); // repetitive read is no problem
        }
        try(ChunkedReader cis2 = new ChunkedReader(Collections.emptyList(), -1)) {
            assertTrue(cis2.read(buffer, 0, 10) == -1);
            assertTrue(cis2.read(buffer, 0, 10) == -1); // repetitive read is no problem
        }
        try(ChunkedReader cis3 = new ChunkedReader(Arrays.asList(null,new char[0]), -1)) {
            assertTrue(cis3.read(buffer, 0, 10) == -1);
            assertTrue(cis3.read(buffer, 0, 10) == -1); // repetitive read is no problem
        }
    }
    @Test
    public void testStreamOfUnknownSize(){
        ChunkedReader cis1 = new ChunkedReader(null, 0);
        assertTrue(cis1.length()==0);
        ChunkedReader cis2 = new ChunkedReader(Collections.emptyList(), -1);
        assertTrue(cis2.length()==0);
        ChunkedReader cis3 = new ChunkedReader(Arrays.asList(null,new char[0]), -1);
        assertTrue(cis3.length()==0);
        ChunkedReader cis4 = new ChunkedReader(Arrays.asList(new char[2]), -1);
        assertTrue(cis4.length()==2);
        ChunkedReader cis5 = new ChunkedReader(Arrays.asList(new char[2], new char[3], new char[4]), -1);
        assertTrue(cis5.length()==9);
    }
    @Test
    public void testOneChunkStreamSingle() throws IOException {
        char[] chunk = new char[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (char)(255-i);
        ChunkedReader cis = new ChunkedReader(chunk);
        assertTrue(cis.length()==chunk.length);
        for (char aChunk : chunk) {
            int val = cis.read();
            assertTrue(val >= 0);
            assertTrue(val == (aChunk & 0xFFFF));
        }
        assertTrue(cis.read() == -1);
    }
    @Test
    public void testOneChunkStreamBuffer() throws IOException {
        char[] chunk = new char[255];
        for(int i=0; i<chunk.length; i++) chunk[i] = (char)(255-i);
        ChunkedReader cis = new ChunkedReader(chunk);
        assertTrue(cis.length()==chunk.length);
        char[] buffer = new char[4];
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
        ArrayList<char[]> chunks = new ArrayList<>();
        int size = 0;
        for(int i=1; i<=40; i++){
            char[] chunk = new char[i];
            chunks.add(chunk);
            for(int j=0; j<chunk.length; j++){
                chunk[j] = (char)size++;
            }
        }
        ChunkedReader cis = new ChunkedReader(chunks, -1);
        assertTrue(cis.length()==size);
        int total = 0;
        while(total<size){
            int val = cis.read();
            assertTrue(val>=0);
            assertTrue(val == (total&0xFFFF));
            total++;
        }
        assertTrue(cis.read() == -1);
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
        ChunkedReader cis = new ChunkedReader(chunks, -1);
        assertTrue(cis.length()==size);

        char[] buffer = new char[4];
        int total = 0;
        int val;
        for(;;){
            val = cis.read(buffer,1,2);
            if (val<=0){
                break;
            }
            assertTrue(val<=2);
            assertTrue((buffer[1]&0xFFFF) == (total&0xFFFF));
            total++;
            if (val>1) {
                assertTrue((buffer[2]&0xFFFF) == (total&0xFFFF));
                total++;
            }
        }
        assertTrue(val == -1);
        assertTrue(total == size);
        assertTrue(cis.read(buffer,1,2) == -1);

    }

}
