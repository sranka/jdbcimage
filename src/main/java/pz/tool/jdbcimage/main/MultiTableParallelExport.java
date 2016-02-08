package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MultiTableParallelExport extends SingleTableExport{
	
	public void run(){
		long start = System.currentTimeMillis();
		List<String> tables;
		try {
			tables = Files.lines(Paths.get(tool_table_file))
					.filter(x -> !x.contains("."))
					.collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// print platform parallelism, just FYI
		int parallelism = getParallelism(tables.size());
		out.println("-- Parallelism "+ parallelism);
		
		// create a queue to rake table names from
		LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(tables);
		queue.addAll(tables);
		
		// runs export in parallel
		ExecutorService taskExecutor = new ForkJoinPool(parallelism,
				ForkJoinPool.defaultForkJoinWorkerThreadFactory,
				null,
				true);
		AtomicBoolean canContinue = new AtomicBoolean(true);

		List<Future<Void>> results = new ArrayList<Future<Void>>();
		for(int i=0; i<parallelism; i++){
			results.add(taskExecutor.submit(new Callable<Void>(){
				@Override
				public Void call() throws Exception {
					String tableName = null;
					try{
						while(canContinue.get() && (tableName = queue.poll()) != null){
							long start = System.currentTimeMillis();
							exportTable(tableName);
							out.println(tableName + " : " + Duration.ofMillis(System.currentTimeMillis()-start));
						}
						tableName = null;
					} finally {
						if (tableName!=null){
							// exception state, notify other threads to stop reading from queue
							canContinue.compareAndSet(true, false);
						}
					}
					return null;
				}
			}));
		}

		// wait for the executor to finish
		taskExecutor.shutdown();
		try {
			taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			// TODO log
			throw new RuntimeException(e);
		}

		// check for exceptions
		for(Future<Void> execution: results){
			try {
				execution.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO log
				throw new RuntimeException(e);
			}
		}
		
		out.println("Total processing time: "+ Duration.ofMillis(System.currentTimeMillis()-start));
		out.println("Files saved to: "+new File(tool_builddir));
	}
    
	public static void main(String... args) throws Exception{
		try(MultiTableParallelExport tool = new MultiTableParallelExport()){tool.run();}
	}
}
