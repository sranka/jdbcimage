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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

import pz.tool.jdbcimage.LoggedUtils;

public abstract class MainToolBase implements AutoCloseable {
	//////////////////////
	// Parameters
	//////////////////////
	public String url = System.getProperty("url");
	public String user = System.getProperty("user");
	public String password = System.getProperty("password");
	// single export tools
	public String tool_table = System.getProperty("tool_table");
	// multi export/import tools
	public boolean tool_ignoreEmptyTables = Boolean.valueOf(System.getProperty("tool_ignoreEmptyTables", "false"));
	public boolean tool_disableIndexes = Boolean.valueOf(System.getProperty("tool_disableIndexes", "false"));
	private String tool_builddir = System.getProperty("tool_builddir");
	private String tool_listeners = System.getProperty("listeners");
	public String zipFile = null;
	// let you connect profiling tools
	public boolean tool_waitOnStartup = Boolean.valueOf(System.getProperty("tool_waitOnStartup", "false"));
	public int tool_concurrency;
	// internal
	public boolean requireZipArgument = Boolean.valueOf(System.getProperty("requireZipArgument", "true"));



	{
		// concurrency
		String prop = System.getProperty("tool_concurrency");
		if (prop == null || prop.length() == 0){
			tool_concurrency = -1;
		} else{
			tool_concurrency = Integer.valueOf(prop);
		}
	}

	/**
	 * Setups zip file from command line arguments supplied.
	 *
	 * @param args arguments.
	 */
	public void setupZipFile(String[] args) {
		if (args.length > 0) {
			if (!args[0].endsWith(".zip")) {
				throw new IllegalArgumentException("zip file expected, but: " + args[0]);
			}
			buildDirectory = new File(
					getBuildDirectory(),
					String.valueOf(System.currentTimeMillis()));
			tool_builddir = buildDirectory.toString();
			if (!buildDirectory.mkdirs()) {
				LoggedUtils.ignore("Unable to create directory " + tool_builddir, null);
			}
			zipFile = args[0];
		} else if (requireZipArgument){
			throw new IllegalArgumentException("zip file is expected as a first argument");
		}
	}

	private File buildDirectory = null;
	public File getBuildDirectory(){
		if (buildDirectory == null){
			if (tool_builddir == null || tool_builddir.trim().isEmpty()){
				buildDirectory = new File(System.getProperty("java.io.tmpdir", "target")+"/tmp_jdbctool");
			} else{
				buildDirectory = new File(tool_builddir);
			}
		}
		return buildDirectory;
	}



	/**
	 * Zips files in the build directory to a configured zipFile.
	 */
	public void zip() {
		if (zipFile != null) {
			long start = System.currentTimeMillis();
			try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile))) {
				zos.setLevel(ZipOutputStream.STORED);
				byte[] buffer = new byte[4096];
				Files.list(Paths.get(tool_builddir))
						.forEach(x -> {
							File f = x.toFile();
							if (f.isFile() && !f.getName().contains(".")) {
								try (FileInputStream fis = new FileInputStream(f)) {
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
				out.println("Zipped to '" + zipFile + "' - " + Duration.ofMillis(System.currentTimeMillis() - start));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Deletes build directory.
	 */
	public void deleteBuildDirectory() {
		if (buildDirectory!=null && buildDirectory.exists()) {
			long start = System.currentTimeMillis();
			try {
				Files.list(Paths.get(tool_builddir))
						.forEach(x -> {
							File f = x.toFile();
							if (!f.delete()) {
								LoggedUtils.ignore("Unable to delete " + f, null);
							}
						});
				if (!buildDirectory.delete()) {
					LoggedUtils.ignore("Unable to delete " + buildDirectory, null);
				}
			} catch (Exception e) {
				LoggedUtils.ignore("Unable to delete " + buildDirectory, e);
			}
			out.println("Delete build directory time - " + Duration.ofMillis(System.currentTimeMillis() - start));
		}
	}

	/**
	 * Unzip files.
	 */
	public void unzip() {
		//extract
		if (zipFile != null) {
			try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
				long start = System.currentTimeMillis();
				byte[] buffer = new byte[4096];
				ZipEntry nextEntry;
				while ((nextEntry = zis.getNextEntry()) != null) {
					try (FileOutputStream fos = new FileOutputStream(new File(getBuildDirectory(), nextEntry.getName()))) {
						int count;
						while ((count = zis.read(buffer)) >= 0) {
							fos.write(buffer, 0, count);
						}
						zis.closeEntry();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				out.println("Unzipped '" + zipFile + "' - " + Duration.ofMillis(System.currentTimeMillis() - start));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Gets a configured concurrency, but at most max number.
	 *
	 * @param max max number to return or non-positive number to ignore
	 * @return concurrency number
	 */
	public int currentConcurrencyLimit(int max) {
		int retVal = tool_concurrency;

		if (retVal <= 0) {
			retVal = Runtime.getRuntime().availableProcessors(); //ForkJoinPool.getCommonPoolParallelism();
		}
		if (max <= 0) {
			return retVal;
		} else {
			return retVal > max ? max : retVal;
		}
	}

	public boolean isIgnoreEmptyTables() {
		return tool_ignoreEmptyTables;
	}

	//////////////////////
	// State
	//////////////////////
	protected PrintStream out = null;
	protected DataSource dataSource = null;
	protected int concurrency = 1; // filled by the started method
	protected long started; // filled by the started method
	protected DBFacade dbFacade = null;
	// tables to import, key is database table name, value is the expected output/input file name
	protected Map<String,String> tables = null;

	public MainToolBase() {
		initOutput();
		started();
		initDataSource();
	}

	protected void started() {
		if (tool_waitOnStartup) {
			out.println("Paused on startup, press ENTER to continue ...");
			try {
				//noinspection ResultOfMethodCallIgnored
				System.in.read();
			} catch (IOException e) {
				// should never occur
				throw new RuntimeException(e);
			}
		}
		concurrency = currentConcurrencyLimit(-1);
		started = System.currentTimeMillis();
		out.println("Started - " + new Date(started));
		if (url!=null && url.length()>0) out.println("Database URL: "+url);
		if (user!=null && user.length()>0) out.println("Database user: "+user);
	}

	protected void setTables(Map<String,String> tables, PrintStream out) {
		this.tables = tables;
		// check for duplicated except of case-sensitiveness
		final boolean[] foundDuplicate = {false};
		Map<String,String> ignoreCaseMap = new HashMap<>();
		tables.forEach((key,value) -> {
			String toPut = key.toLowerCase();
			String old = ignoreCaseMap.put(toPut, value);
			if (old!=null){
				out.println("Two tables with same case-insensitive name: "+old+" and "+ key);
				foundDuplicate[0] = true;
			}
		});
		if (foundDuplicate[0]){
			throw new RuntimeException("Found tables with the same case-insensitive name, see the log above. Drop/rename tables so there are no conflicts!");
		}

	}

	public boolean containsTable(String tableName) {
		return tables.containsKey(tableName);
	}

	public void close() {
		if (zipFile != null) {
			deleteBuildDirectory();
		}
		finished();
		if (dataSource != null) {
			try {
				((BasicDataSource) dataSource).close();
			} catch (SQLException e) {
				LoggedUtils.ignore("Unable to close data source!", e);
			}
		}
	}

	protected void finished() {
		out.println("Total processing time - " + Duration.ofMillis(System.currentTimeMillis() - started));
		out.println("Finished - " + new Date(System.currentTimeMillis()));
	}

	protected void initOutput() {
		this.out = System.out;
	}

	protected void initDataSource() {
		BasicDataSource bds = new BasicDataSource();
		bds.setUrl(url);
		bds.setUsername(user);
		bds.setPassword(password);
		bds.setDefaultAutoCommit(false);

		// isolate database specific instructions
		List<Predicate<String>> matchers = Arrays.asList(
				dbtype -> url.startsWith("jdbc:"+dbtype),
				dbtype -> url.contains(":"+dbtype+":")
		);
		for(Predicate<String> matcher: matchers){
			if (matcher.test("oracle")) {
				dbFacade = new Oracle();
			} else if (matcher.test("sqlserver")){
				dbFacade = new Mssql();
			} else if (matcher.test("postgresql")) {
				dbFacade = new PostgreSQL();
			} else if (matcher.test("mysql") || matcher.test("mariadb")) {
				dbFacade = new MariaDB();
			}
			if (dbFacade !=null) break;
		}
		if (dbFacade == null){
			throw new IllegalArgumentException("Unsupported database type: " + url);
		}

		dbFacade.setToolBase(this);
		dbFacade.addListeners(DBFacadeListener.getInstances(tool_listeners));
		LoggedUtils.info("Tool listeners: "+dbFacade.listeners);
		dbFacade.setupDataSource(bds);
		dataSource = bds;
	}

	/**
	 * Gets a list of tables in the current catalog.
	 *
	 * @return list of tables
	 */
	public List<String> getUserTables() {
		try (Connection con = getReadOnlyConnection()) {
			return dbFacade.getUserTables(con);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public OutputStream toResultOutput(File f) throws FileNotFoundException {
		return new DeflaterOutputStream(new FileOutputStream(f));
	}

	public InputStream toResultInput(File f) throws FileNotFoundException {
		boolean zip = false;
		if (!f.exists() || (zip = f.getName().endsWith(".zip"))) {
			int sep = -1;
			String zipFile = "";
			if (zip || (sep = f.getName().indexOf("#")) > 0) {
				if (sep > 0) {
					zipFile = f.getName().substring(sep + 1);
					f = new File(f.getParent(), f.getName().substring(0, sep));
				}
				if (f.getName().endsWith(".zip") && f.exists()) {
					try {
						ZipInputStream zis = new ZipInputStream(new FileInputStream(f));
						ZipEntry entry;
						while ((entry = zis.getNextEntry()) != null) {
							if (entry.getName().equalsIgnoreCase(zipFile)) {
								return new InflaterInputStream(zis);
							}
							zis.closeEntry();
						}
						zis.close();
						// re-read to offer names
						out.println("Run dump with the following argument to dump a specific table: ");
						zis = new ZipInputStream(new FileInputStream(f));
						List<String> entries = new ArrayList<String>();
						while ((entry = zis.getNextEntry()) != null) {
							entries.add(entry.getName());
							zis.closeEntry();
						}
						final File file = f;
						entries.stream().sorted().forEach( x -> {
								out.print(" ");
								out.print(file);
								out.print("#");
								out.println(x);
						});
						out.println();
						if (!zipFile.isEmpty()){
							throw new IllegalArgumentException("No file named '"+zipFile+"' found in: " + f);
						}
						zis.close();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			throw new IllegalArgumentException("File not found: " + f);
		}
		return new InflaterInputStream(new FileInputStream(f));
	}

	public Connection getReadOnlyConnection() throws SQLException {
		Connection con = dataSource.getConnection();
		con.setReadOnly(true);

		return con;
	}

	public Connection getWriteConnection() throws SQLException {
		Connection con = dataSource.getConnection();
		con.setReadOnly(false);

		return con;
	}

	public Supplier<Connection> getWriteConnectionSupplier() {
		return () -> {
			try {
				return getWriteConnection();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		};
	}

	/**
	 * Run the specified tasks concurrently or serially depending on configured concurrency.
	 *
	 * @param tasks tasks to execute
	 * @throws RuntimeException if any of the tasks failed.
	 */
	public void run(List<Callable<?>> tasks) {
		if (concurrency <= 1) {
			runSerial(tasks);
		} else {
			runConcurrently(tasks);
		}
	}

	/**
	 * Run the specified tasks in the current thread.
	 *
	 * @param tasks tasks to execute
	 * @throws RuntimeException if any of the tasks failed.
	 */
	public void runSerial(List<Callable<?>> tasks) {
		for (Callable<?> t : tasks) {
			try {
				t.call();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Run the specified tasks concurrently and wait for them to finish.
	 *
	 * @param tasks tasks to execute
	 * @throws RuntimeException if any of the tasks failed.
	 */
	public void runConcurrently(List<Callable<?>> tasks) {
		// OPTIMIZED concurrent execution using a queue to quickly take tasks from
		LinkedBlockingQueue<Callable<?>> queue = new LinkedBlockingQueue<>(tasks);

		ExecutorService taskExecutor = new ForkJoinPool(concurrency,
				ForkJoinPool.defaultForkJoinWorkerThreadFactory,
				null,
				true);
		AtomicBoolean canContinue = new AtomicBoolean(true);

		List<Future<Void>> results = new ArrayList<>();
		for (int i = 0; i < concurrency; i++) {
			results.add(taskExecutor.submit(() -> {
				Callable<?> task = null;
				try {
					while (canContinue.get() && (task = queue.poll()) != null) {
						task.call();
					}
					task = null;
				} finally {
					if (task != null) {
						// exception state, notify other threads to stop reading from queue
						canContinue.compareAndSet(true, false);
					}
				}
				return null;
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
		for (Future<?> execution : results) {
			try {
				execution.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Executes a query and maps results.
	 *
	 * @param query  query to execute
	 * @param mapper function to map result set rows
	 * @return list of results
	 * @throws SQLException db error
	 */
	public <T> List<T> executeQuery(String query, Function<ResultSet, T> mapper) throws SQLException {
		try (Connection con = getReadOnlyConnection()) {
			List<T> retVal = new ArrayList<>();
			try (Statement stmt = con.createStatement()) {
				try (ResultSet rs = stmt.executeQuery(query)) {
					while (rs.next()) {
						T result = mapper.apply(rs);
						if (result != null) {
							retVal.add(result);
						}
					}
				}
			} finally {
				try {
					con.rollback(); // nothing to commit
				} catch (SQLException e) {
					LoggedUtils.ignore("Unable to rollback!", e);
				}
			}
			return retVal;
		}
	}

	public static String[] setupSystemProperties(String... args){
		if (args == null) args = new String[0];
		ArrayList<String> retVal = new ArrayList<>(args.length);
		int index;
		for(String s: args){
			if (s.startsWith("-") && (index=s.indexOf('=',1)) > 0){
				System.setProperty(s.substring(1,index),s.substring(index+1));
			} else{
				retVal.add(s);
			}
		}
		return retVal.toArray(new String[retVal.size()]);
	}
}
