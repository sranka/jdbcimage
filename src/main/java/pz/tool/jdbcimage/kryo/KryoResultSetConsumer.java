package pz.tool.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Output;
import pz.tool.jdbcimage.ResultConsumer;
import pz.tool.jdbcimage.ResultSetInfo;
import pz.tool.jdbcimage.main.Mssql;
import pz.tool.jdbcimage.main.Oracle;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;

/**
 * Serializes the result set into the supplied output stream.
 *
 * @author zavora
 */
public class KryoResultSetConsumer implements ResultConsumer<ResultSet> {
    // allows serializing sql_variant type, where a specific type information is required
    private static final HashMap<Class<?>, Integer> SQL_VARIANT_CLASS_TO_TYPE;
    static {
        SQL_VARIANT_CLASS_TO_TYPE = new HashMap<>();
        SQL_VARIANT_CLASS_TO_TYPE.put(Long.class, Types.BIGINT);
        SQL_VARIANT_CLASS_TO_TYPE.put(String.class, Types.VARCHAR);
        SQL_VARIANT_CLASS_TO_TYPE.put(byte[].class, Types.BINARY);
        SQL_VARIANT_CLASS_TO_TYPE.put(Boolean.class, Types.BIT);
        SQL_VARIANT_CLASS_TO_TYPE.put(Date.class, Types.DATE);
        SQL_VARIANT_CLASS_TO_TYPE.put(Time.class, Types.TIME);
        SQL_VARIANT_CLASS_TO_TYPE.put(Timestamp.class, Types.TIMESTAMP);
        SQL_VARIANT_CLASS_TO_TYPE.put(BigDecimal.class, Types.DECIMAL);
        SQL_VARIANT_CLASS_TO_TYPE.put(Double.class, Types.DOUBLE);
        SQL_VARIANT_CLASS_TO_TYPE.put(Integer.class, Types.INTEGER);
        SQL_VARIANT_CLASS_TO_TYPE.put(Short.class, Types.TINYINT);
        SQL_VARIANT_CLASS_TO_TYPE.put(Float.class, Types.REAL);
    }

    // serialization
    private final Kryo kryo;
    private final Output out;

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
                Class<?> clazz = null;
                Serializer<?> serializer = null;

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
                    case Mssql.Types.SQL_VARIANT:
                        val = rs.getObject(i + 1);
                        if (val == null) {
                            clazz = String.class; // any class is good for serializing null
                        } else {
                            // check that the class is supported
                            clazz = val.getClass();
                            Integer type = SQL_VARIANT_CLASS_TO_TYPE.get(clazz);
                            if (type == null ){
                                throw new IllegalStateException("Unable to serialize sql_variant SQL type: " + info.types[i]
                                        + ", Class: " + clazz.getName()
                                        + ", Object: " + val);
                            }
                            out.writeInt(type);
                        }
                        break;
                    default:
                        val = rs.getObject(i + 1);
                        throw new IllegalStateException("Unable to serialize SQL type: " + info.types[i]
                                + ", Class: " + (val == null ? "<unknown>" : val.getClass().getName())
                                + ", Object: " + val);
                }
                if (rs.wasNull()) {
                    val = null;
                }
                if (clazz != null) {
                    kryo.writeObjectOrNull(out, val, clazz);
                } else if (serializer != null) {
                    kryo.writeObjectOrNull(out, val, serializer);
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
