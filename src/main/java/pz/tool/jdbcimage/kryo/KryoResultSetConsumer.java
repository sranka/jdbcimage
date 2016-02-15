package pz.tool.jdbcimage.kryo;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.FastOutput;

import pz.tool.jdbcimage.ResultConsumer;
import pz.tool.jdbcimage.ResultSetInfo;

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
	
	public KryoResultSetConsumer(OutputStream out) {
		super();
		this.kryo = KryoSetup.getKryo();
		this.out = new FastOutput(out);
	}

	@Override
	public void onStart(ResultSetInfo info){
		this.info = info;
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
					case Types.CHAR:
					case Types.VARCHAR:
					case Types.LONGVARCHAR: // TODO move to CLOB
						kryo.writeObjectOrNull(out, rs.getString(i+1), String.class);
					case Types.NCHAR:
					case Types.LONGNVARCHAR: // TODO move to CLOB
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
					case Types.BLOB:
						Blob blob = rs.getBlob(i+1);
						if (rs.wasNull()) blob = null;
						kryo.writeObjectOrNull(out, blob, KryoBlobSerializer.INSTANCE);
						break;
					case Types.CLOB: // TODO CLOB handling
					case Types.NCLOB: // TODO CLOB handling
					default:
						throw new IllegalStateException("Unable to serialize SQL type: "+info.types[i]+", Object: "+rs.getObject(i+1));
				}
			}
		} catch(SQLException e){
			//Unable to recover from any error
			throw new RuntimeException(e);
		}
	}

	@Override
	public void onFinish() {
		// end of rows
		out.writeBoolean(false);
		// flush buffer
		out.flush();
	}
}
