package io.github.sranka.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.Input;
import io.github.sranka.jdbcimage.ResultProducer;
import io.github.sranka.jdbcimage.ResultSetInfo;
import io.github.sranka.jdbcimage.RowData;
import io.github.sranka.jdbcimage.main.Mssql;
import io.github.sranka.jdbcimage.main.Oracle;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import static io.github.sranka.jdbcimage.kryo.KryoResultSetConsumer.VERSION_1_0;

/**
 * Pull-style produced with all row data out of the supplied input stream.
 */
public class KryoResultProducer implements ResultProducer {
    // serialization
    private final Kryo kryo;
    private final Input in;

    // state
    private int[] types;
    private boolean finished = false;
    private boolean isVersion1_0 = false;

    public KryoResultProducer(InputStream in) {
        super();
        this.kryo = KryoSetup.getKryo();
        this.in = new FastInput(in);
    }

    @Override
    public RowData start() {
        // skip version information
        String version = kryo.readObject(in, String.class);
        isVersion1_0 = version.equals(VERSION_1_0);
        // prepare new row data
        ResultSetInfo info = kryo.readObject(in, ResultSetInfo.class);
        types = new int[info.types.length];
        System.arraycopy(info.types, 0, types, 0, types.length);
        return new RowData(info);
    }

    @Override
    public boolean fillData(RowData row) {
        // check if we reached the end
        if (finished) return false;
        if (!in.readBoolean()) {
            finished = true;
            return false;
        }
        // fill in row
        for (int i = 0; i < types.length; i++) {
            Object val;
            int dbType = types[i];
            if (dbType == Mssql.Types.SQL_VARIANT || dbType == Types.OTHER) {
                // read a specific type stored within sql_variant
                dbType = in.readInt();
            }
            switch (dbType) {
                case Types.BIGINT:
                    val = kryo.readObjectOrNull(in, Long.class);
                    break;
                case Types.BINARY:
                    val = kryo.readObjectOrNull(in, byte[].class);
                    break;
                case Types.BIT:
                    val = kryo.readObjectOrNull(in, Boolean.class);
                    break;
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.NVARCHAR:
                case Mssql.Types.DATETIMEOFFSET:
                    val = kryo.readObjectOrNull(in, String.class);
                    break;
                case Types.DATE:
                    if (isVersion1_0) {
                        // produces https://github.com/sranka/jdbcimage/issues/19
                        val = kryo.readObjectOrNull(in, Date.class);
                    } else {
                        val = kryo.readObjectOrNull(in, String.class);
                        if (val != null) {
                            val = Date.valueOf((String)val);
                        }
                    }
                    break;
                case Types.TIME:
                    if (isVersion1_0) {
                        // produces https://github.com/sranka/jdbcimage/issues/19
                        val = kryo.readObjectOrNull(in, Time.class);
                    } else {
                        val = kryo.readObjectOrNull(in, String.class);
                        if (val != null) {
                            val = Time.valueOf((String)val);
                        }
                    }
                    break;
                case Types.TIMESTAMP:
                    val = kryo.readObjectOrNull(in, Timestamp.class);
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    val = kryo.readObjectOrNull(in, BigDecimal.class);
                    break;
                case Types.DOUBLE:
                case Oracle.Types.BINARY_DOUBLE:
                    val = kryo.readObjectOrNull(in, Double.class);
                    break;
                case Types.INTEGER:
                    val = kryo.readObjectOrNull(in, Integer.class);
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                    val = kryo.readObjectOrNull(in, Short.class);
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    val = kryo.readObjectOrNull(in, Float.class);
                    break;
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    val = KryoInputStreamSerializer.INSTANCE.deserializeBlobData(in, row.info.connection);
                    break;
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    val = KryoReaderSerializer.INSTANCE.deserializeClobData(in, row.info.connection);
                    break;
                case Types.LONGNVARCHAR:
                case Types.NCLOB:
                    val = KryoReaderSerializer.INSTANCE.deserializeNClobData(in, row.info.connection);
                    break;
                default:
                    throw new IllegalStateException("Unable to deserialize object for SQL type: " + types[i]
                            + (dbType != types[i] ? ("/" + dbType) : "")
                    );
            }
            row.values[i] = val;
        }

        return true;
    }

    @Override
    public void close() {
        // nothing to close herein
    }

}
