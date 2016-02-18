package pz.tool.jdbcimage.kryo;

import com.esotericsoftware.kryo.Kryo;

import pz.tool.jdbcimage.ResultSetInfo;

/**
 * Setups and caches kryo instances. 
 */
public class KryoSetup {
	private static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
	    protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			kryo.setReferences(false);
			kryo.register(ResultSetInfo.class);
			return kryo;
	    }
	};
	/**
	 * Returns a kryo instance to use.
	 * @return kryo instance
	 */
	public static Kryo getKryo(){
		return kryos.get();
	}
}
