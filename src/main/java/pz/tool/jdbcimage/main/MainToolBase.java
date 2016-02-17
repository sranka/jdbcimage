package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

import pz.tool.jdbcimage.LoggedUtils;
import pz.tool.jdbcimage.db.SqlExecuteCommand;
import pz.tool.jdbcimage.db.TableGroupedCommands;

public abstract class MainToolBase implements AutoCloseable{
	//////////////////////
	// Parameters
	//////////////////////
	public String jdbc_url = System.getProperty("jdbc_url","jdbc:oracle:thin:@localhost:1521:XE");
	public String jdbc_user = System.getProperty("jdbc_user","hpem");
	public String jdbc_password = System.getProperty("jdbc_password","changeit");
	// single export tools
	public String tool_table = System.getProperty("tool_table","ry_syncMeta");
	// multi export/import tools
	public boolean tool_ignoreEmptyTables = Boolean.valueOf(System.getProperty("tool_ignoreEmptyTables","false"));
	public boolean tool_disableIndexes = Boolean.valueOf(System.getProperty("tool_disableIndexes","false"));
	public String tool_builddir = System.getProperty("tool_builddir","target/export");
	public String zipFile = null;
	// parallelism
	public int tool_parallelism = Integer.valueOf(System.getProperty("tool_parallelism","-1"));
	// let you connect profiling tools
	public boolean tool_waitOnStartup=Boolean.valueOf(System.getProperty("tool_waitOnStartup","false"));
	
	/**
	 * Setups zip file from command line arguments supplied.
	 * @param args arguments.
	 */
	public void setupZipFile(String[] args){
		if (args.length>0){
			if (!args[0].endsWith(".zip")){
				throw new IllegalArgumentException("zip file expected, but: "+args[0]);
			}
			tool_builddir = new File(
					tool_builddir,
					String.valueOf(System.currentTimeMillis())).toString();
			new File(tool_builddir).mkdirs();
			zipFile = args[0];
		}
	}
	
	/**
	 * Zip files.
	 * @param directory directory with files
	 * @param zipFile zip to create
	 */
	public void zip(){
		if (zipFile!=null){
			long start = System.currentTimeMillis();
			try(ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile))){
				zos.setLevel(ZipOutputStream.STORED);
				byte[] buffer = new byte[4096];
				Files.list(Paths.get(tool_builddir))
					.forEach(x -> {
						File f = x.toFile();
						if (f.isFile() && !f.getName().contains(".")){
							try (FileInputStream fis = new FileInputStream(f)){
								ZipEntry zipEntry = new ZipEntry(f.getName());
								zos.putNextEntry(zipEntry);
								int count;
								while ((count = fis.read(buffer)) >= 0) {
									zos.write(buffer, 0, count);
								}
								zos.closeEntry();
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
					});
				out.println("Zipped to '"+zipFile+"' - "+ Duration.ofMillis(System.currentTimeMillis()-start));
			} catch(IOException e){
				throw new RuntimeException(e);
			}
		}
	}
	
	/**
	 * Delete build directory.
	 * @param directory directory to delete
	 */
	public void deleteBuildDirectory(){
		File dir = new File(tool_builddir);
		if (dir.exists()){
			long start = System.currentTimeMillis();
			try {
				Files.list(Paths.get(tool_builddir))
				.forEach(x -> {
					File f = x.toFile();
					f.delete();
				});
				dir.delete();
			} catch (Exception e) {
				LoggedUtils.ignore("Unable to delete "+tool_builddir, e);
			}
			out.println("Delete build directory time - "+ Duration.ofMillis(System.currentTimeMillis()-start));
		}
	}
	/**
	 * Unzip files.
	 * @param directory directory to write to
	 * @param zipFile zip to read from
	 */
	public void unzip(){
		//extract
		if (zipFile!=null){
			try(ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))){
				long start = System.currentTimeMillis();
				byte[] buffer = new byte[4096];
				ZipEntry nextEntry;
				while((nextEntry = zis.getNextEntry())!=null){
					try (FileOutputStream fos = new FileOutputStream (new File(tool_builddir,nextEntry.getName()))){
						int count;
						while ((count = zis.read(buffer)) >= 0) {
							fos.write(buffer, 0, count);
						}
						zis.closeEntry();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				out.println("Unzipped '"+zipFile+"' - "+ Duration.ofMillis(System.currentTimeMillis()-start));
			} catch(IOException e){
				throw new RuntimeException(e);
			}
		}
	}

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
	protected DBFacade dbFacade= null;
	// tables to import
	protected List<String> tables = null;
	protected Map<String,String> tableSet;
	
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

	protected void setTables(List<String> tables){
		this.tables = tables;
		this.tableSet = new HashMap<>();
		for(String t: tables){
			tableSet.put(t, t);
		}
	}
	public boolean containsTable(String tableName){
		return tableSet.containsKey(tableName);
	}

	public void close(){
		if (zipFile!=null){
			deleteBuildDirectory();
		}
		finished();
		if (dataSource!=null){
			try {
				((BasicDataSource)dataSource).close();
			} catch (SQLException e) {
				LoggedUtils.ignore("Unable to close data source!", e);
			}
		}
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

		// isolate database specific instructions
		if (jdbc_url.startsWith("jdbc:oracle")){
			dbFacade = new Oracle();
		} else if (jdbc_url.startsWith("jdbc:sqlserver")){
			dbFacade = new Mssql();
		} else{
			throw new IllegalArgumentException("Unsupported database type: "+jdbc_url);
		}

		dbFacade.setupDataSource(bds);
		dataSource = bds;
	}
	
	/**
	 * Gets a list of tables in the current catalog.
	 * @return list of tables
	 */
	public List<String> getUserTables(){
		try(Connection con = getReadOnlyConnection()){
			List<String> retVal = new ArrayList<>();
			// for Oracle: schema = currentUser.toUpperCase()
			try(ResultSet tables = dbFacade.getUserTables(con)){
				while(tables.next()){
					String tableName = tables.getString(3);
					retVal.add(tableName);
				}
			}
			return retVal;
		} catch(Exception e){
			throw new RuntimeException(e);
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
	public Supplier<Connection> getWriteConnectionSupplier(){
		return () -> {
			try{
				return getWriteConnection();
			} catch(SQLException e){
				throw new RuntimeException(e);
			}
		};
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
	
	/**
	 * Executes a query and maps results.
	 * @param query query to execute
	 * @param mapper function to map result set rows
	 * @return list of results
	 * @throws SQLException
	 */
	public <T> List<T> executeQuery(String query, Function<ResultSet,T> mapper) throws SQLException{
		try(Connection con = getReadOnlyConnection()){
			List<T> retVal = new ArrayList<T>();
			try(Statement stmt = con.createStatement()){
				try(ResultSet rs = stmt.executeQuery(query)){
					while(rs.next()){
						T result = mapper.apply(rs);
						if (result!=null){
							retVal.add(result);
						}
					}
				}
			} finally{
				try {
					con.rollback(); // nothing to commit
				} catch (SQLException e) {
					LoggedUtils.ignore("Unable to rollback!", e);
				}
			}
			return retVal;
		}
	}

	
	///////////////////////////////////////
	// Database specific instructions
	///////////////////////////////////////

	/**
	 * Facade that isolates specifics of a particular database 
	 * in regards to operations used by import/export.
	 * 
	 * @author zavora
	 */
	public static abstract class DBFacade{
		/**
		 * Setups data source.
		 * @param bds
		 */
		public abstract void setupDataSource(BasicDataSource bds);
		/**
		 * Gets a result set representing current user user tables.
		 * @param con connection
		 * @return result
		 */
		public abstract ResultSet getUserTables(Connection con) throws SQLException;

		/**
		 * Turns on/off table constraints.
		 * @param enable
		 * @return operation time
		 * @throws SQLException
		 */
		public abstract Duration modifyConstraints(boolean enable) throws SQLException;
		
		/**
		 * Turns on/off table indexes.
		 * @param enable
		 * @return operation time
		 * @throws SQLException
		 */
		public abstract Duration modifyIndexes(boolean enable) throws SQLException;

		/**
		 * Called before rows are inserted into table.
		 * @param con connection
		 * @param table table name
		 * @param hasIdentityColumn indicates whether the table has identity column
		 */
		public void afterImportTable(Connection con, String table, boolean hasIdentityColumn) throws SQLException{
		}

		/**
		 * Called before rows are inserted into table.
		 * @param con connection
		 * @param table table name
		 * @param hasIdentityColumn indicates whether the table has identity column
		 */
		public void beforeImportTable(Connection con, String table, boolean hasIdentityColumn) throws SQLException{
		}
		/**
		 * Gets the SQL DML that truncates the content of a table.
		 * @param tableName table
		 * @return command to execute
		 */
		public String getTruncateTableSql(String tableName){
			return "TRUNCATE TABLE "+escapeTableName(tableName);			
		}
		
		/**
		 * Escapes column name
		 * @param s s
		 * @return escaped column name so that it can be used in queries.
		 */
		public String escapeColumnName(String s){
			return s;
		}
		/**
		 * Escapes table name
		 * @param s s
		 * @return escaped table name so that it can be used in queries.
		 */
		public String escapeTableName(String s){
			return s;
		}
		
		/**
		 * Returns tables that have no identity columns.
		 * @return
		 */
		public Set<String> getTablesWithIdentityColumns() {
			return Collections.emptySet();
		}
	}
	public class Oracle extends DBFacade{
		@Override
		public void setupDataSource(BasicDataSource bds) {
			List<String> connectionInits = Arrays.asList(
					"ALTER SESSION ENABLE PARALLEL DDL", //could possibly make index disabling quicker
					"ALTER SESSION SET skip_unusable_indexes = TRUE" //avoid ORA errors caused by unusable indexes
					);
			bds.setConnectionInitSqls(connectionInits);
			// the minimum level supported by Oracle
			bds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		}

		@Override
		public ResultSet getUserTables(Connection con) throws SQLException{
			return con.getMetaData().getTables(con.getCatalog(),jdbc_user.toUpperCase(),"%",new String[]{"TABLE"});
		}

		@Override
		public String escapeColumnName(String s){
			return "\""+s+"\"";
		}

		@Override
		public String escapeTableName(String s){
			return "\""+s+"\"";
		}
		
		@Override
		public Duration modifyConstraints(boolean enable) throws SQLException{
			long time = System.currentTimeMillis();
			String[] conditions;
			if (enable){
				// on enable: enable foreign indexes after other types
				conditions = new String[]{"CONSTRAINT_TYPE<>'R'","CONSTRAINT_TYPE='R'"};
			} else{
				// on disable: disable foreign indexes first  
				conditions = new String[]{"CONSTRAINT_TYPE='R'","CONSTRAINT_TYPE<>'R'"};
			}
			TableGroupedCommands commands = new TableGroupedCommands();
			for(int i=0; i<2; i++){
				executeQuery(
							"SELECT OWNER,TABLE_NAME,CONSTRAINT_NAME FROM user_constraints WHERE "+conditions[i]+" order by TABLE_NAME",
							row -> {
								try {
									String owner = row.getString(1);
									String tableName = row.getString(2);
									String constraint = row.getString(3);
									if (containsTable(tableName)){
										if (enable){
											String desc = "Enable constraint "+constraint+" on table "+tableName;
											String sql = "ALTER TABLE "+owner+"."+tableName
											 + " MODIFY CONSTRAINT "+constraint+" ENABLE";
											commands.add(tableName, desc, sql);
										} else{
											String desc = "Disable constraint "+constraint+" on table "+tableName;
											String sql = "ALTER TABLE "+owner+"."+tableName
											 + " MODIFY CONSTRAINT "+constraint+" DISABLE";
											commands.add(tableName, desc, sql);
										}
										return null;
									} else{
										return null;
									}
								} catch (SQLException e) {
									throw new RuntimeException(e);
								}
							}
						);
				run(commands.tableGroups
						.stream()
						.map(x -> SqlExecuteCommand.toSqlExecuteTask(
								getWriteConnectionSupplier(),
								out,
								x.toArray(new SqlExecuteCommand[x.size()]))
								)
						.collect(Collectors.toList()));
			}
			return Duration.ofMillis(System.currentTimeMillis()-time);
		}

		@Override
		public Duration modifyIndexes(boolean enable) throws SQLException{
			long time = System.currentTimeMillis();
			TableGroupedCommands commands = new TableGroupedCommands();
			executeQuery(
					    /** exclude LOB indexes, since they cannot be altered */
						"SELECT TABLE_OWNER,TABLE_NAME,INDEX_NAME FROM user_indexes where INDEX_TYPE<>'LOB' order by TABLE_NAME",
						row -> {
							try {
								String owner = row.getString(1);
								String tableName = row.getString(2);
								String index = row.getString(3);
								if (containsTable(tableName)){
									if (enable){
										String desc = "Rebuild index "+index+" on table "+tableName;
										String sql = "ALTER INDEX "+owner+"."+index
												// SHOULD BE "REBUILD ONLINE" ... but it works only on Enterprise Edition on oracle
										          + " REBUILD"; 
										commands.add(tableName, desc, sql);
									} else{
										String desc = "Disable index "+index+" on table "+tableName;
										String sql = "ALTER INDEX "+owner+"."+index
												   +" UNUSABLE";
										commands.add(tableName, desc, sql);
									}
									return null;
								} else{
									return null;
								}
							} catch (SQLException e) {
								throw new RuntimeException(e);
							}
						}
					);
			run(commands.tableGroups
					.stream()
					.map(x -> SqlExecuteCommand.toSqlExecuteTask(
							getWriteConnectionSupplier(),
							out,
							x.toArray(new SqlExecuteCommand[x.size()]))
							)
					.collect(Collectors.toList()));
			return Duration.ofMillis(System.currentTimeMillis()-time);
		}
	}
	public class Mssql extends DBFacade{
		@Override
		public void setupDataSource(BasicDataSource bds) {
			bds.setDefaultTransactionIsolation(Connection.TRANSACTION_NONE);
		}
		@Override
		public ResultSet getUserTables(Connection con) throws SQLException{
			return con.getMetaData().getTables(con.getCatalog(),"dbo","%",new String[]{"TABLE"});		
		}
		@Override
		public String escapeColumnName(String s){
			return "["+s+"]";
		}
		@Override
		public String escapeTableName(String s){
			return "["+s+"]";
		}
		@Override
		public Duration modifyConstraints(boolean enable) throws SQLException {
			long time = System.currentTimeMillis();
			List<String> queries = new ArrayList<>();
			// table name, foreign key name
			queries.add("SELECT t.Name, dc.Name "
					+   "FROM sys.tables t INNER JOIN sys.foreign_keys dc ON t.object_id = dc.parent_object_id "
					+   "ORDER BY t.Name");
			TableGroupedCommands commands = new TableGroupedCommands();
			for(String query: queries){
				executeQuery(
							query,
							row -> {
								try {
									String tableName = row.getString(1);
									String constraint = row.getString(2);
									if (containsTable(tableName)){
										if (enable){
											String desc = "Enable constraint "+constraint+" on table "+tableName;
											String sql = "ALTER TABLE ["+tableName+"] CHECK CONSTRAINT ["+constraint+"]";
											commands.add(tableName, desc, sql);
										} else{
											String desc = "Disable constraint "+constraint+" on table "+tableName;
											String sql = "ALTER TABLE ["+tableName+"] NOCHECK CONSTRAINT ["+constraint+"]";
											commands.add(tableName, desc, sql);
										}
										return null;
									} else{
										return null;
									}
								} catch (SQLException e) {
									throw new RuntimeException(e);
								}
							}
						);
				// there are DEADLOCK problems when running in parallel
				runSerial(commands.tableGroups
						.stream()
						.map(x -> SqlExecuteCommand.toSqlExecuteTask(
								getWriteConnectionSupplier(),
								out,
								x.toArray(new SqlExecuteCommand[x.size()]))
								)
						.collect(Collectors.toList()));
			}
			return Duration.ofMillis(System.currentTimeMillis()-time);
		
		}
		@Override
		public Duration modifyIndexes(boolean enable) throws SQLException {
			long time = System.currentTimeMillis();
			out.println("Index "+(enable?"enable":"disable")+" not supported on MSSQL!");
			return Duration.ofMillis(System.currentTimeMillis()-time);
		}
		@Override
		public String getTruncateTableSql(String tableName){
			// unable to use TRUNCATE TABLE on MSSQL server even with CONSTRAINTS DISABLED!
			return "DELETE FROM "+escapeTableName(tableName);			
		}
		
		@Override
		public void afterImportTable(Connection con, String table, boolean hasIdentityColumn) throws SQLException{
			if (hasIdentityColumn){
				try(Statement stmt = con.createStatement()){
					stmt.execute("SET IDENTITY_INSERT ["+table+"] OFF");
				}
			}
		}

		@Override
		public void beforeImportTable(Connection con, String table, boolean hasIdentityColumn) throws SQLException{
			if (hasIdentityColumn){
				try(Statement stmt = con.createStatement()){
					stmt.execute("SET IDENTITY_INSERT ["+table+"] ON");
				}
			}
		}
		/**
		 * Returns tables that have no identity columns.
		 * @return
		 */
		public Set<String> getTablesWithIdentityColumns() {
			Set<String> retVal = new HashSet<>();
			try(Connection con = getReadOnlyConnection()){
				try(Statement stmt = con.createStatement()){
					try(ResultSet rs = stmt.executeQuery("select name from sys.objects where type = 'U' and OBJECTPROPERTY(object_id, 'TableHasIdentity')=1")){
						while(rs.next()){
							retVal.add(rs.getString(1));
						}
					}
				} finally{
					try {
						con.rollback(); // nothing to commit
					} catch (SQLException e) {
						LoggedUtils.ignore("Unable to rollback!", e);
					}
				}
			} catch(SQLException e){
				throw new RuntimeException(e);
			}
			return retVal;
		}
	}
}
