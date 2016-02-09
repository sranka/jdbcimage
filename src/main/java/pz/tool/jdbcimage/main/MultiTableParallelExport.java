package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * DB export that runs in multiple threads.
 */
public class MultiTableParallelExport extends SingleTableExport{
	
	public void run(){
		List<String> tables;
		try {
			tables = Files.lines(Paths.get(tool_table_file))
					.filter(x -> !x.contains("."))
					.collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// print platform parallelism, just FYI
		out.println("-- Parallelism "+ parallelism);
		
		// runs export in parallel
		run(tables.stream().map(x -> getExportTask(x)).collect(Collectors.toList()));
		
		out.println("Files saved to: "+new File(tool_builddir));
	}
	
	private Callable<?> getExportTask(String tableName){
		return new Callable<Void>(){
			@Override
			public Void call() throws Exception {
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
			}
		};		
	}
    
	public static void main(String... args) throws Exception{
		try(MultiTableParallelExport tool = new MultiTableParallelExport()){tool.run();}
	}
}
