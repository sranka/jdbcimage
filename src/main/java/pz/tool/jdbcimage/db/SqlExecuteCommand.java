package pz.tool.jdbcimage.db;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Described SQL Command to execute. 
 */
public class SqlExecuteCommand{
	public String description;
	public String sql;

	public SqlExecuteCommand(String description, String sql) {
		this.description = description;
		this.sql = sql;
	}
	
	public static Callable<Void> toSqlExecuteTask(Supplier<Connection> connectionSupplier, PrintStream out, SqlExecuteCommand... commands){
		return new Callable<Void>(){
			@Override
			public Void call() throws Exception {
				try(Connection con = connectionSupplier.get()){
					boolean failed = true;
					SqlExecuteCommand last = null;
					try(Statement stmt = con.createStatement()){
						for(SqlExecuteCommand cmd: commands){
							last = cmd;
							stmt.execute(last.sql);
							if (out!=null) out.println("SUCCESS: "+last.description);
						}
						failed = false;
					} finally{
						try {
							if (failed) {
								if (last!=null && out!=null) out.println("FAILURE: "+last.description);
								con.rollback(); // nothing to commit
							} else{
								con.commit();
							}
						} catch (SQLException e) {
							// TODO log
						}
					}
					return null;
				}
			}
		};
	}
	
}