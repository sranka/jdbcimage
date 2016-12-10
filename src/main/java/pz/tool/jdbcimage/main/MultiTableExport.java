package pz.tool.jdbcimage.main;

/**
 * DB Export that runs in a single thread.
 */
public class MultiTableExport extends MultiTableParallelExport{
	
	@Override
	protected void started() {
		super.started();
		concurrency = 1;
	}
	
	public static void main(String... args) throws Exception{
		try(MultiTableExport tool = new MultiTableExport()){tool.run();}
	}
}
