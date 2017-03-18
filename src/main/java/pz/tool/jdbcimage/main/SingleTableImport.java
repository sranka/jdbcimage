package pz.tool.jdbcimage.main;

import pz.tool.jdbcimage.LoggedUtils;
import pz.tool.jdbcimage.ResultProducerRunner;
import pz.tool.jdbcimage.db.DbImportResultConsumer;
import pz.tool.jdbcimage.kryo.KryoResultProducer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Import a single table from the database.  
 * @author zavora
 */
public class SingleTableImport extends MainToolBase{

	public void run() throws SQLException, IOException{
		out.println("Importing from: "+new File(tool_builddir, tool_table));
		out.println("Reset time: "+ truncateTable(tool_table));
		long time = System.currentTimeMillis();
		out.println("Imported rows: "+ importTable(tool_table, tool_table, dbFacade.getTablesWithIdentityColumn().get(tool_table)));
		out.println("Import time: "+ Duration.ofMillis(System.currentTimeMillis()-time));
	}
	
	public Duration truncateTable(String tableName) throws SQLException {
		long start = System.currentTimeMillis();
		Connection con = getWriteConnection();
		boolean committed = false;
		try{
			// delete table
			try(Statement del = con.createStatement()){
				del.executeUpdate(dbFacade.getTruncateTableSql(tableName));
			}
			con.commit();
			committed = true;
			return Duration.ofMillis(System.currentTimeMillis()-start);
		} finally{
			if (!committed){
				try{con.rollback();} catch(Exception e){LoggedUtils.ignore("Unable to rollback!",e);}
			}
			LoggedUtils.close(con);
		}
	}

	/**
	 * Imports specific tables.
	 * @param tableName tables name
	 * @param fileName usually the same table name, might differ in lower/upper case
	 * @param identityInfo identity column information
	 * @return time spent
	 */
	public long importTable(String tableName, String fileName, Object identityInfo) throws SQLException, IOException{
		File file = new File(tool_builddir, fileName);
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
			dbFacade.beforeImportTable(con, tableName, identityInfo);
			ResultProducerRunner runner = new ResultProducerRunner(producer, new DbImportResultConsumer(tableName, con, dbFacade, actualColumns));
			long rows = runner.run();
			dbFacade.afterImportTable(con, tableName, identityInfo);

			return rows;
		} finally{
			LoggedUtils.close(con);
			LoggedUtils.close(in);
		}
	}
	
	public static void main(String... args) throws Exception{
		try(SingleTableImport tool = new SingleTableImport()){tool.run();}
	}

}
