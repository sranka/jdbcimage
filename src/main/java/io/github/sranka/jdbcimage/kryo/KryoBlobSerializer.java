package io.github.sranka.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Kryo serializer for JDBC BLOB instances.
 * @author zavora
 *
 */
public class KryoBlobSerializer extends Serializer<Blob>{
	public static KryoBlobSerializer INSTANCE = new KryoBlobSerializer();

	@Override
	public Blob read(Kryo kryo, Input in, Class<Blob> type) {
		// not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(Kryo kryo, Output out, Blob value) {
		try {
			InputStream in = value.getBinaryStream();
			KryoInputStreamSerializer.INSTANCE.write(kryo, out, in);
			// free the blob
			value.free();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
