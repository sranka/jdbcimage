package pz.tool.jdbcimage.kryo;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.Input;

import pz.tool.jdbcimage.ResultProducer;
import pz.tool.jdbcimage.ResultSetInfo;
import pz.tool.jdbcimage.RowData;

/**
 * Pull-style produced with all row data out of the supplied input stream.
 * @author zavora
 */
public class KryoResultProducer implements ResultProducer{
	// serialization
	private Kryo kryo;
	private Input in;
	
	// state
	private int[] types;
	private boolean finished = false;

	public KryoResultProducer(InputStream in) {
		super();
		this.kryo = KryoSetup.getKryo();
		this.in = new FastInput(in);
	}
	
	@Override
	public RowData start() {
		// skip version information
		kryo.readObject(in, String.class);
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
		if (!in.readBoolean()){
			finished = true;
			return false;
		}
		// fill in row
		for(int i=0; i<types.length; i++){
			Object val;
			switch(types[i]){
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
					val = kryo.readObjectOrNull(in, String.class);
					break;
				case Types.DATE:
					val = kryo.readObjectOrNull(in, Date.class);
					break;
				case Types.TIME:
					val = kryo.readObjectOrNull(in, Time.class);
					break;
				case Types.TIMESTAMP:
					val = kryo.readObjectOrNull(in, Timestamp.class);
					break;
				case Types.DECIMAL:
				case Types.NUMERIC:
					val = kryo.readObjectOrNull(in, BigDecimal.class);
					break;
				case Types.DOUBLE:
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
					throw new IllegalStateException("Unable to deserialize object for SQL type: "+types[i]);
			}
			row.values[i] = val;
		}
		
		return true;
	}

	@Override
	public void close() {
		// nothing to close
	}

}
