package io.github.sranka.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Output;
import io.github.sranka.jdbcimage.ResultConsumer;
import io.github.sranka.jdbcimage.ResultSetInfo;
import io.github.sranka.jdbcimage.main.Mssql;
import io.github.sranka.jdbcimage.main.Oracle;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Serializes the result set into the supplied output stream.
 */
public class KryoResultSetConsumer implements ResultConsumer<ResultSet> {
    public static final String VERSION_1_0 = "1.0";
    public static final String VERSION_1_1 = "1.1";

    public static final byte TIME_TYPE_NULL = 0;
    public static final byte TIME_TYPE_EXACT = 1;
    public static final byte TIME_TYPE_LOCAL = 2;

    private static final Calendar CALENDAR_LOCAL = Calendar.getInstance();
    private static final Calendar CALENDAR_OTHER = Calendar.getInstance(TimeZone.getTimeZone("GMT+0130"));
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
        kryo.writeObject(out, VERSION_1_1);
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
                    case Types.BOOLEAN:
                    case Types.BIT:
                        val = rs.getBoolean(i + 1);
                        clazz = Boolean.class;
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Mssql.Types.DATETIMEOFFSET:
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
                        // version 1.0 was:
                        // clazz = Date.class;
                        // version 1.1:
                        if (val != null) {
                            val = val.toString();
                        }
                        clazz = String.class;
                        break;
                    case Types.TIME:
                        val = rs.getTime(i + 1);
                        // version 1.0 was:
                        // clazz = Time.class;
                        // version 1.1:
                        clazz = String.class;
                        if (val != null) {
                            val = val.toString();
                        }
                        break;
                    case Types.TIMESTAMP:
                        // version 1.0 was:
                        // val = rs.getTimestamp(i + 1);
                        // clazz = Timestamp.class;
                        // version 1.1:
                        val = rs.getTimestamp(i + 1, CALENDAR_LOCAL);
                        clazz = String.class;
                        if (val == null) {
                            out.writeByte(TIME_TYPE_NULL); // storing null value
                        } else {
                            Timestamp val1 = (Timestamp) val;
                            Timestamp val2 = rs.getTimestamp(i + 1, CALENDAR_OTHER);
                            if (val1.getTime() == val2.getTime()) {
                                // exact timestamp is specified, store nanos and millis
                                out.writeByte(TIME_TYPE_EXACT);
                                out.writeLong(val1.getTime());
                                out.writeInt(val1.getNanos());
                                val = null;
                            } else {
                                // timestamp is a local datetime
                                out.writeByte(TIME_TYPE_LOCAL); // storing null value
                                val = val1.toString();
                            }
                        }
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
                            out.writeInt(Types.VARCHAR);
                        } else {
                            // check that the class is supported
                            clazz = val.getClass();
                            Integer type = SQL_VARIANT_CLASS_TO_TYPE.get(clazz);
                            if (type == null) {
                                throw new IllegalStateException("Unable to serialize sql_variant SQL type: " + info.types[i]
                                        + ", Class: " + clazz.getName()
                                        + ", Object: " + val);
                            }
                            out.writeInt(type);
                        }
                        break;
                    case Types.OTHER:
                        val = rs.getObject(i + 1);
                        if (val == null) {
                            clazz = String.class; // any class is good for serializing null
                            out.writeInt(Types.VARCHAR);
                        } else if (val.getClass().getName().equals("org.postgresql.util.PGobject")) {
                            // PGObject serialized as a string
                            clazz = String.class;
                            val = val.toString();
                            out.writeInt(Types.VARCHAR);
                        } else if (val instanceof UUID) {
                            clazz = String.class;
                            val = val.toString();
                            out.writeInt(Types.VARCHAR);
                        } else {
                            throw new IllegalStateException("Unable to serialize SQL OTHER type: " + info.types[i]
                                    + ", Class: " + val.getClass().getName()
                                    + ", Object: " + val);
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
