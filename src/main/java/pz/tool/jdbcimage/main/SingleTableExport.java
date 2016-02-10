package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;

import pz.tool.jdbcimage.QueryRunner;
import pz.tool.jdbcimage.kryo.KryoResultSetConsumer;

/**
 * Performs export of a single table.
 * @author zavora
 */
public class SingleTableExport extends MainToolBase{

	public void run() throws SQLException, IOException{
		out.println("Processing time: "+ exportTable(tool_table));
		out.println("Saved to: "+new File(tool_builddir, tool_table));
	}
	
	public Duration exportTable(String tableName) throws SQLException, IOException{
		File file = new File(tool_builddir, tableName);
		OutputStream out = toResultOutput(file);
		KryoResultSetConsumer serializer = new KryoResultSetConsumer(out);
		boolean failed = true;

		Connection con = getReadOnlyConnection();
		QueryRunner runner = null;
		try{
			runner = new QueryRunner(con, getSelectStatement(tableName, con), serializer);
			runner.run();
			failed = false;
			return runner.getDuration();
		} finally{
			try{con.close();} catch(Exception e){/* TODO log */};
			// close the file
			try{out.close();} catch(Exception e){/* TODO log */};
			// delete the output if it failed or zero rows read
			if (failed || (runner.rows == 0 && isIgnoreEmptyTables())) {
				file.delete();
			}
		}
	}
	
	public String getSelectStatement(String tableName, Connection con) throws SQLException{
		// get column names, BLOBs must be last to avoid
		// ORA-24816: Expanded non LONG bind data supplied
		StringBuilder columns = new StringBuilder();
		boolean hasId = false;
		try(Statement stmt = con.createStatement()){
			try(ResultSet rs = stmt.executeQuery("SELECT * FROM "+escapeTableName(tableName)+" WHERE 0=1")){
				ResultSetMetaData meta = rs.getMetaData();
				int columnCount = meta.getColumnCount();
				boolean needComma = false; 
				for(int i=0; i<columnCount; i++){
					if ("id".equalsIgnoreCase(meta.getColumnName(i+1))){
						hasId = true;
					}
					if (meta.getColumnType(i+1) != Types.BLOB){
						if (needComma) columns.append(","); else needComma=true;
						columns.append(escapeColumnName(meta.getColumnName(i+1)));
					};
				}
				for(int i=0; i<columnCount; i++){
					if (meta.getColumnType(i+1) == Types.BLOB){
						if (needComma) columns.append(","); else needComma=true;
						columns.append(escapeColumnName(meta.getColumnName(i+1)));
					};
				}
			}
		}
		
		String retVal = "SELECT "+columns+" FROM "+escapeTableName(tableName);
		if (hasId){
			retVal += " ORDER BY id";
		}
		return retVal;
	}
	
	public static void main(String... args) throws Exception{
		try(SingleTableExport tool = new SingleTableExport()){tool.run();}
	}
}
