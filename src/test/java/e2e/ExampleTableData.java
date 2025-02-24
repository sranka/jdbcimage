package e2e;

import io.github.sranka.jdbcimage.ChunkedReader;
import io.github.sranka.jdbcimage.RowData;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.Assert.assertArrayEquals;

public class ExampleTableData {
    public static final long UPDATED_AT_MILLIS = Instant.parse("2024-01-01T12:00:00.0Z").toEpochMilli();
    private final Object[] data = new Object[]{
            1,
            "John Doe",
            "This is a sample description.",
            30,
            new BigDecimal("99.99"),
            true,
            Timestamp.valueOf("2024-01-01 12:00:00.0"),
            new Timestamp(UPDATED_AT_MILLIS),
            Date.valueOf("1993-05-20"),
            Time.valueOf("14:30:00"),
            "{\"key\": \"value\"}",
            "192.168.1.1",
            "550e8400-e29b-41d4-a716-446655440000"
    };

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
        // lowercase UUID for comparison, MSSQL exports in uppercase but can import independently on case
        if (toCompare.length >= rowData.values.length) {
            toCompare[12] = toCompare[12].toString().toLowerCase();
        }
        // normalize timestamp as millis
        if (data[7] != null && toCompare[7] instanceof Timestamp) {
            data[7] = UPDATED_AT_MILLIS;
            toCompare[7] = ((Timestamp)toCompare[7]).getTime();
        }
        assertArrayEquals(this.data, toCompare);
    }
}
