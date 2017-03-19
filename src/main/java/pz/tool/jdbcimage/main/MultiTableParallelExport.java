package pz.tool.jdbcimage.main;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DB export that runs in multiple threads.
 */
public class MultiTableParallelExport extends SingleTableExport{
	
	public void run(){
		// print platform concurrency, just FYI
		out.println("Concurrency: "+ concurrency);

		// setup tables to export
		setTables(getUserTables().stream().collect(Collectors.toMap(Function.identity(), Function.identity())), out);


		// runs export in parallel
		out.println("Exporting table files to: "+ getBuildDirectory());
		run(tables.entrySet().stream().map(x -> getExportTask(x.getKey(), x.getValue())).collect(Collectors.toList()));
		zip();
	}
	
	private Callable<?> getExportTask(String tableName, String fileName){
		return () -> {
			boolean failed = true;
			try{
				long start = System.currentTimeMillis();
				long rows = exportTable(tableName, new File(getBuildDirectory(), fileName));
				out.println("SUCCESS: Exported table "+tableName + " - "+rows+" rows in " + Duration.ofMillis(System.currentTimeMillis()-start));
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
		//noinspection UnusedAssignment
		args = setupSystemProperties(args);

		try(MultiTableParallelExport tool = new MultiTableParallelExport()){
			tool.setupZipFile(args);
			tool.run();
		}
	}
}
