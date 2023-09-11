package pz.tool.jdbcimage.db;

import org.junit.Test;

import static org.junit.Assert.*;

public class BinaryZerosRemoverTest {

    @Test
    public void shouldRemoveAllBinaryZerosFromString() {
        BinaryZerosRemover bzr = new BinaryZerosRemover();

        assertEquals("abcdefghijkl", bzr.removeBinaryZeros("\0abc\0def\0ghi\u0000jkl"));
    }

    @Test
    public void shouldNotFailOnNulls() {
        BinaryZerosRemover bzr = new BinaryZerosRemover();

        assertNull(bzr.removeBinaryZeros(null));
    }
}