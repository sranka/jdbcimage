package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
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
		try{
			QueryRunner runner = new QueryRunner(con, getSelectStatement(tableName), serializer);
			runner.run();
			failed = false;
			return runner.getDuration();
		} finally{
			try{con.close();} catch(Exception e){/* TODO log */};
			// close the file
			try{out.close();} catch(Exception e){/* TODO log */};
			// delete the output if it failed
			if (failed) file.delete();
		}
	}
	
	public String getSelectStatement(String tableName){
		String retVal = "SELECT * FROM "+tableName;
		String upper = tableName.toUpperCase();
		if ("RY_COLLECTION".equals(upper) || "PM_VALIDATIONRESULT".equals(upper) || "RY_TAXCATEGORY".equals(upper)){
			retVal += " ORDER BY id";
			
		}
		return retVal;
	}
	
	public static void main(String... args) throws Exception{
		try(SingleTableExport tool = new SingleTableExport()){tool.run();}
	}
}
