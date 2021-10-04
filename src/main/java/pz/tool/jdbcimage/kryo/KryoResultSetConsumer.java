package pz.tool.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Output;
import pz.tool.jdbcimage.ResultConsumer;
import pz.tool.jdbcimage.ResultSetInfo;
import pz.tool.jdbcimage.main.Oracle;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Serializes the result set into the supplied output stream.
 *
 * @author zavora
 */
public class KryoResultSetConsumer implements ResultConsumer<ResultSet> {
    // private static Object types

    // serialization
    private Kryo kryo;
    private Output out;

    // initialized in onStart
    private ResultSetInfo info;
    private int columnCount;
    private long processedRows = -1;

    public KryoResultSetConsumer(OutputStream out) {
        super();
        this.kryo = KryoSetup.getKryo();
        this.out = new FastOutput(out);
    }

    @Override
    public void onStart(ResultSetInfo info) {
        this.info = info;
        this.processedRows = 0;
        columnCount = info.columns.length;
        kryo.writeObject(out, "1.0"); // 1.0 version of the serialization
        kryo.writeObject(out, info); // write header
    }

    @Override
    public void accept(ResultSet rs) {
        out.writeBoolean(true);// row item
        try {
            // TYPE mapping taken from https://msdn.microsoft.com/en-us/library/ms378878(v=sql.110).aspx
            for (int i = 0; i < columnCount; i++) {
                Object val;
                Class clazz = null;
                Serializer serializer = null;

                switch (info.types[i]) {
                    case Types.BIGINT:
                        val = rs.getLong(i + 1);
                        clazz = Long.class;
                        break;
                    case Types.BINARY:
                        val = rs.getBytes(i + 1);
                        clazz = byte[].class;
                        break;
                    case Types.BIT:
                        val = rs.getBoolean(i + 1);
                        clazz = Boolean.class;
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        val = rs.getString(i + 1);
                        clazz = String.class;
                        break;
                    case Types.NCHAR:
                    case Types.NVARCHAR:
                        val = rs.getNString(i + 1);
                        clazz = String.class;
                        break;
                    case Types.DATE:
                        val = rs.getDate(i + 1);
                        clazz = Date.class;
                        break;
                    case Types.TIME:
                        val = rs.getTime(i + 1);
                        clazz = Time.class;
                        break;
                    case Types.TIMESTAMP:
                        val = rs.getTimestamp(i + 1);
                        clazz = Timestamp.class;
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        val = rs.getBigDecimal(i + 1);
                        clazz = BigDecimal.class;
                        break;
                    case Types.DOUBLE:
                    case Oracle.Types.BINARY_DOUBLE:
                        val = rs.getDouble(i + 1);
                        clazz = Double.class;
                        break;
                    case Types.INTEGER:
                        val = rs.getInt(i + 1);
                        clazz = Integer.class;
                        break;
                    case Types.TINYINT:
                    case Types.SMALLINT:
                        val = rs.getShort(i + 1);
                        clazz = Short.class;
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                        val = rs.getFloat(i + 1);
                        clazz = Float.class;
                        break;
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        val = rs.getBinaryStream(i + 1);
                        serializer = KryoInputStreamSerializer.INSTANCE;
                        break;
                    case Types.BLOB:
                        val = rs.getBlob(i + 1);
                        serializer = KryoBlobSerializer.INSTANCE;
                        break;
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        val = rs.getClob(i + 1);
                        serializer = KryoClobSerializer.INSTANCE;
                        break;
                    case Types.LONGNVARCHAR:
                    case Types.NCLOB:
                        val = rs.getNClob(i + 1);
                        serializer = KryoClobSerializer.INSTANCE;
                        break;
                    default:
                        val = rs.getObject(i + 1);
                        break;
                }
                if (val == null || rs.wasNull()) {
                    kryo.writeObjectOrNull(out, null, String.class);
                } else {
                    if (clazz != null) {
                        kryo.writeObjectOrNull(out, val, clazz);
                    } else if (serializer != null) {
                        kryo.writeObjectOrNull(out, val, serializer);
                    } else {
                        throw new IllegalStateException("Unable to serialize SQL type: " + info.types[i]
                                + ", Class: " + val.getClass().getName()
                                + ", Object: " + val);
                    }
                }
            }

            processedRows++;
        } catch (SQLException e) {
            //Unable to recover from any error
            throw new RuntimeException(e);
        }
    }

    @Override
    public long onFinish() {
        // end of rows
        out.writeBoolean(false);
        // flush buffer
        out.flush();

        return processedRows;
    }
}
