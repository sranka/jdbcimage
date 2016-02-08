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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
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
		
		if (retVal == -1){
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
		out.println("Started - "+ new Date(System.currentTimeMillis()));
	}
	protected void finished(){
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
}
