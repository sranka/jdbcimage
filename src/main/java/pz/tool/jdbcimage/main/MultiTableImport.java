package pz.tool.jdbcimage.main;

/**
 * DB Import that runs in a single thread.
 */
public class MultiTableImport extends MultiTableParallelImport{
	@Override
	protected void started() {
		super.started();
		parallelism = 1;
	}
	
	public static void main(String... args) throws Exception{
		try(MultiTableImport tool = new MultiTableImport()){tool.run();}
	}
}
