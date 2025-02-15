package e2e;

import io.github.sranka.jdbcimage.ChunkedReader;
import io.github.sranka.jdbcimage.RowData;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.Assert.assertArrayEquals;

public class ExampleTableData {
    private final Object[] data = new Object[]{1, "John Doe", "This is a sample description.", 30, new BigDecimal("99.99"), true, Timestamp.valueOf("2024-01-01 12:00:00.0"), Timestamp.valueOf("2024-01-01 12:00:00.0"), Date.valueOf("1993-05-20"), Time.valueOf("14:30:00"), "{\"key\": \"value\"}", "192.168.1.1", "550e8400-e29b-41d4-a716-446655440000"};

    public ExampleTableData ignoreUpdatedAtColumn() {
        data[7] = null;
        return this;
    }

    public void assertEquals(RowData rowData) {
        Object[] toCompare = new Object[rowData.values.length];
        System.arraycopy(rowData.values, 0, toCompare, 0, rowData.values.length);
        for (int i = 0; i < data.length && i < toCompare.length; i++) {
            if (data[i] == null) {
                toCompare[i] = null;
            } else if (toCompare[i] instanceof ChunkedReader) {
                toCompare[i] = ((ChunkedReader) toCompare[i]).readAsString();
            }
        }
        assertArrayEquals(this.data, toCompare);
    }
}
