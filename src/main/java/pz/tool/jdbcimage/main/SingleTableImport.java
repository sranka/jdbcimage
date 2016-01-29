package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import pz.tool.jdbcimage.DbImportResultConsumer;
import pz.tool.jdbcimage.ResultProducerRunner;
import pz.tool.jdbcimage.kryo.KryoResultProducer;

/**
 * Import a single table from the database.  
 * @author zavora
 */
public class SingleTableImport extends MainToolBase{

	public void run() throws SQLException, IOException{
		out.println("Importing from: "+new File(tool_builddir, tool_table));
		out.println("Reset time: "+ resetTable(tool_table));
		out.println("Import time: "+ importTable(tool_table));
	}
	
	public Duration resetTable(String tableName) throws SQLException, IOException{
		long start = System.currentTimeMillis();
		Connection con = getWriteConnection();
		boolean commited = false;
		try{
			// delete table
			try(Statement del = con.createStatement()){
				del.executeUpdate("DELETE FROM "+tableName);
			}
			con.commit();
			commited = true;
			return Duration.ofMillis(System.currentTimeMillis()-start);
		} finally{
			if (!commited){
				try{con.rollback();} catch(Exception e){/* TODO */};
			}
			try{con.close();} catch(Exception e){/* TODO */};
		}
	}
	public Duration importTable(String tableName) throws SQLException, IOException{
		File file = new File(tool_builddir, tableName);
		InputStream in = toResultInput(file);
		KryoResultProducer producer = new KryoResultProducer(in);

		Connection con = getWriteConnection();

		try{
			// detect actual columns, ignore case
			Map<String, String> actualColumns = new HashMap<>(); 
			try(Statement detect = con.createStatement()){
				try(ResultSet rs = detect.executeQuery("SELECT * FROM "+tableName+" WHERE 0=1")){
					ResultSetMetaData metaData = rs.getMetaData();
					int cols = metaData.getColumnCount();
					for(int i=1; i<=cols; i++){
						String colName = metaData.getColumnName(i);
						actualColumns.put(colName.toLowerCase(), colName);
					}
				}
			} finally{
				con.rollback(); // no changes
			}
			// import data
			ResultProducerRunner runner = new ResultProducerRunner(producer, new DbImportResultConsumer(tableName, con, actualColumns));
			runner.run();
			return runner.getDuration();
		} finally{
			try{con.close();} catch(Exception e){/* TODO */};
			// close the file
			try{in.close();} catch(Exception e){/* TODO */};
		}
	}
	
	public static void main(String... args) throws Exception{
		try(SingleTableImport tool = new SingleTableImport()){tool.run();}
	}

}
