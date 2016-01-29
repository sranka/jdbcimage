package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
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
	public String tool_builddir = System.getProperty("tool_builddir","target/export");
	public String tool_table_file = System.getProperty("tool_table_file","src/test/resources/exampleTables.txt");
	// dump file
	public String tool_in_file = System.getProperty("tool_in_file","target/export/ry_resource");
	public String tool_out_file = System.getProperty("tool_out_file","target/ry_resource.dump");

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
