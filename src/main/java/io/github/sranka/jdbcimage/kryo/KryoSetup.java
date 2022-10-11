package io.github.sranka.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;
import io.github.sranka.jdbcimage.ResultSetInfo;

/**
 * Setups and caches kryo instances. 
 */
public class KryoSetup {
	private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() ->  {
			Kryo kryo = new Kryo();
			kryo.setReferences(false);
			kryo.register(ResultSetInfo.class);
			return kryo;
	    });
	/**
	 * Returns a kryo instance to use.
	 * @return kryo instance
	 */
	public static Kryo getKryo(){
		return kryoThreadLocal.get();
	}
}
