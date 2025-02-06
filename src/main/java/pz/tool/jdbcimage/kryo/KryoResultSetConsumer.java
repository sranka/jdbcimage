package pz.tool.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Output;
import pz.tool.jdbcimage.ResultConsumer;
import pz.tool.jdbcimage.ResultSetInfo;

import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.*;

/**
 * Serializes the result set into the supplied output stream.
 * @author zavora
 */
public class KryoResultSetConsumer implements ResultConsumer<ResultSet>{
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
	public void onStart(ResultSetInfo info){
		this.info = info;
		this.processedRows = 0;
		columnCount = info.columns.length;
		kryo.writeObject(out, "1.0"); // 1.0 version of the serialization
		kryo.writeObject(out, info); // write header
	}

	@Override
	public void accept(ResultSet rs){
		out.writeBoolean(true);// row item
		try{
			// TYPE mapping taken from https://msdn.microsoft.com/en-us/library/ms378878(v=sql.110).aspx
			for(int i=0; i<columnCount; i++){
	 			switch (info.types[i]) {
					case Types.BIGINT:
						Long longVal = rs.getLong(i+1);
						if (rs.wasNull()) longVal = null;
						kryo.writeObjectOrNull(out, longVal, Long.class);
						break;
					case Types.BINARY:
						kryo.writeObjectOrNull(out, rs.getBytes(i+1), byte[].class);
						break;
					case Types.BIT:
						Boolean bVal = rs.getBoolean(i+1);
						if (rs.wasNull()) bVal = null;
						kryo.writeObjectOrNull(out, bVal, Boolean.class);
						break;
                    case Types.OTHER:
					case Types.CHAR:
					case Types.VARCHAR:
						kryo.writeObjectOrNull(out, rs.getString(i+1), String.class);
						break;
					case Types.NCHAR:
					case Types.NVARCHAR:
						kryo.writeObjectOrNull(out, rs.getNString(i+1), String.class);
						break;
					case Types.DATE:
						kryo.writeObjectOrNull(out, rs.getDate(i+1), Date.class);
						break;
					case Types.TIME:
						kryo.writeObjectOrNull(out, rs.getTime(i+1), Time.class);
						break;
					case Types.TIMESTAMP:
						kryo.writeObjectOrNull(out, rs.getTimestamp(i+1), Timestamp.class);
						break;
					case Types.DECIMAL:
					case Types.NUMERIC:
						kryo.writeObjectOrNull(out, rs.getBigDecimal(i+1), BigDecimal.class);
						break;
					case Types.DOUBLE:
						Double dVal = rs.getDouble(i+1);
						if (rs.wasNull()) dVal = null;
						kryo.writeObjectOrNull(out, dVal, Double.class);
						break;
					case Types.INTEGER:
						Integer intVal = rs.getInt(i+1);
						if (rs.wasNull()) intVal = null;
						kryo.writeObjectOrNull(out, intVal, Integer.class);
						break;
					case Types.TINYINT:
					case Types.SMALLINT:
						Short sVal = rs.getShort(i+1);
						if (rs.wasNull()) sVal = null;
						kryo.writeObjectOrNull(out, sVal, Short.class);
						break;
					case Types.REAL:
					case Types.FLOAT:
						Float fVal = rs.getFloat(i+1);
						if (rs.wasNull()) fVal = null;
						kryo.writeObjectOrNull(out, fVal, Float.class);
						break;
					case Types.VARBINARY:
					case Types.LONGVARBINARY:
						InputStream binaryStream = rs.getBinaryStream(i + 1);
						if (rs.wasNull()) binaryStream = null;
						kryo.writeObjectOrNull(out, binaryStream, KryoInputStreamSerializer.INSTANCE);
						break;
					case Types.BLOB:
						Blob blob = rs.getBlob(i+1);
						if (rs.wasNull()) blob = null;
						kryo.writeObjectOrNull(out, blob, KryoBlobSerializer.INSTANCE);
						break;
					case Types.LONGVARCHAR:
					case Types.CLOB:
						Clob clob = rs.getClob(i+1);
						if (rs.wasNull()) clob = null;
						kryo.writeObjectOrNull(out, clob, KryoClobSerializer.INSTANCE);
						break;
					case Types.LONGNVARCHAR:
					case Types.NCLOB:
						Clob nclob = rs.getNClob(i+1);
						if (rs.wasNull()) nclob = null;
						kryo.writeObjectOrNull(out, nclob, KryoClobSerializer.INSTANCE);
						break;
					default:
						throw new IllegalStateException("Unable to serialize SQL type: "+info.types[i]+", Object: "+rs.getObject(i+1));
				}
			}
			processedRows++;
		} catch(SQLException e){
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
