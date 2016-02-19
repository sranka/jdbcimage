package pz.tool.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Kryo serializer for JDBC BLOB instances.
 * @author zavora
 *
 */
public class KryoClobSerializer extends Serializer<Clob>{
	public static KryoClobSerializer INSTANCE = new KryoClobSerializer();

	@Override
	public Clob read(Kryo kryo, Input in, Class<Clob> type) {
		// not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(Kryo kryo, Output out, Clob value) {
		try {
			Reader in = value.getCharacterStream();
			KryoReaderSerializer.INSTANCE.write(kryo, out, in);
			// free the blob
			value.free();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
