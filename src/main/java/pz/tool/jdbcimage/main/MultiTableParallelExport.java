package pz.tool.jdbcimage.main;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * DB export that runs in multiple threads.
 */
public class MultiTableParallelExport extends SingleTableExport{
	
	public void run(){
		// setup tables to export
		setTables(getUserTables());

		// print platform parallelism, just FYI
		out.println("-- Parallelism "+ parallelism);
		
		// runs export in parallel
		out.println("Exporting table files to: "+new File(tool_builddir));
		run(tables.stream().map(this::getExportTask).collect(Collectors.toList()));
		zip();
	}
	
	private Callable<?> getExportTask(String tableName){
		return () -> {
			boolean failed = true;
			try{
				long start = System.currentTimeMillis();
				exportTable(tableName);
				out.println("SUCCESS: Exported table "+tableName + " - " + Duration.ofMillis(System.currentTimeMillis()-start));
				failed = false;
			} finally {
				if (failed){
					// exception state, notify other threads to stop reading from queue
					out.println("FAILURE: Export of table "+tableName);
				}
			}
			return null;
		};
	}
    
	public static void main(String... args) throws Exception{
		try(MultiTableParallelExport tool = new MultiTableParallelExport()){
			tool.setupZipFile(args);
			tool.run();
		}
	}
}
