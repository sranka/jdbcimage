package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

public abstract class MainToolBase implements AutoCloseable{
	//////////////////////
	// Parameters
	//////////////////////
	public String jdbc_url = System.getProperty("jdbc_url","jdbc:oracle:thin:@localhost:1521:XE");
	public String jdbc_user = System.getProperty("jdbc_user","hpem");
	public String jdbc_password = System.getProperty("jdbc_password","changeit");
	// single export tools
	public String tool_table = System.getProperty("tool_table","ry_resource");
	// multi export/import tools
	public boolean tool_ignoreEmptyTables = Boolean.valueOf(System.getProperty("tool_ignoreEmptyTables","false"));
	public boolean tool_disableIndexes = Boolean.valueOf(System.getProperty("tool_disableIndexes","false"));
	public String tool_builddir = System.getProperty("tool_builddir","target/export");
	public String tool_table_file = System.getProperty("tool_table_file","src/test/resources/exampleTables.txt");
	// dump file
	public String tool_in_file = System.getProperty("tool_in_file","target/export/ry_resource");
	public String tool_out_file = System.getProperty("tool_out_file","target/ry_resource.dump");
	// parallelism
	public int tool_parallelism = Integer.valueOf(System.getProperty("tool_parallelism","-1"));
	// let you connect profiling tools
	public boolean tool_waitOnStartup=Boolean.valueOf(System.getProperty("tool_waitOnStartup","false"));
	/**
	 * Gets a number of configured parallel threads, but at most max number.
	 * @param max max number to return or non-positive number to ignore
	 * @return parallelism number 
	 */
	public int getParallelism(int max){
		int retVal = tool_parallelism;
		
		if (retVal <=0 ){
			retVal = ForkJoinPool.getCommonPoolParallelism();
		}
		if (max<=0){
			return retVal;
		} else{
			return retVal>max?max:retVal;
		}
	}
	
	public boolean isIgnoreEmptyTables(){
		return tool_ignoreEmptyTables;
	}
	
	//////////////////////
	// State
	//////////////////////
	protected PrintStream out = null;
	protected DataSource dataSource = null;
	protected int parallelism = 1; // filled by the started method
	protected long started; // filled by the started method 
	
	public MainToolBase(){
		initOutput();
		started();
		initDataSource();
	}
	
	protected void started(){
		if (tool_waitOnStartup){
			out.println("Paused on startup, press ENTER to continue ...");
			try {
				System.in.read();
			} catch (IOException e) {
				// nothing to do
			}
		}
		parallelism = getParallelism(-1);
		started = System.currentTimeMillis();
		out.println("Started - "+ new Date(started));
	}
	protected void finished(){
		out.println("Total processing time - "+ Duration.ofMillis(System.currentTimeMillis()-started));
		out.println("Finished - "+ new Date(System.currentTimeMillis()));
	}
	protected void initOutput(){
		this.out = System.out;
	}
	protected void initDataSource(){
		BasicDataSource bds = new BasicDataSource();
		bds.setUrl(jdbc_url);
		bds.setUsername(jdbc_user);
		bds.setPassword(jdbc_password);
		bds.setDefaultAutoCommit(false);

		List<String> connectionInits = Arrays.asList(
				"ALTER SESSION ENABLE PARALLEL DDL", //could possibly make index disabling quicker
				"ALTER SESSION SET skip_unusable_indexes = TRUE" //avoid ORA errors caused by unusable indexes
				);
		bds.setConnectionInitSqls(connectionInits);
		// the minimum level supported by Oracle
		bds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

		
		dataSource = bds;
	}

	public void close(){
		finished();
		if (dataSource!=null){
			try {
				((BasicDataSource)dataSource).close();
			} catch (SQLException e) {
				/* TODO log */
			}
		}
	}
	
	public static OutputStream toResultOutput(File f) throws FileNotFoundException{
		return new DeflaterOutputStream(new FileOutputStream(f));
	}
	public static InputStream toResultInput(File f) throws FileNotFoundException{
		return new InflaterInputStream(new FileInputStream(f));
	}
	
	public Connection getReadOnlyConnection() throws SQLException{
		Connection con = dataSource.getConnection();
		con.setReadOnly(true);
		
		return con;
	}
	public Connection getWriteConnection() throws SQLException{
		Connection con = dataSource.getConnection();
		con.setReadOnly(false);
		
		return con;
	}
	/**
	 * Run the specified tasks in parallel or in the current thread depending on configured parallelism.
	 * @param tasks tasks to execute
	 * @throws RuntimeException if any of the tasks failed. 
	 */		
	public void run(List<Callable<?>> tasks){
		if (parallelism<=1){
			runSerial(tasks);
		} else{
			runParallel(tasks);
		}
	}

	/**
	 * Run the specified tasks in the current thread.
	 * @param tasks tasks to execute
	 * @throws RuntimeException if any of the tasks failed. 
	 */		
	public void runSerial(List<Callable<?>> tasks){
		for(Callable<?> t: tasks){
			try {
				t.call();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	/**
	 * Run the specified tasks in parallel and wait for them to finish.
	 * @param tasks tasks to execute
	 * @throws RuntimeException if any of the tasks failed. 
	 */		
	public void runParallel(List<Callable<?>> tasks){
		// OPTIMIZED parallel execution using a queue to quickly take tasks from
		LinkedBlockingQueue<Callable<?>> queue = new LinkedBlockingQueue<>(tasks);

		// runs tasks in parallel
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
					Callable<?> task = null;
					try{
						while(canContinue.get() && (task = queue.poll()) != null){
							task.call();
						}
						task = null;
					} finally {
						if (task!=null){
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
			throw new RuntimeException(e);
		}

		// check for exceptions
		for(Future<?> execution: results){
			try {
				execution.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
