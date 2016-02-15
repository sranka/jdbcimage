package pz.tool.jdbcimage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;


/**
 * A simple class that runs a specified query and pushes the each row to a specified consumer. 
 * @author zavora
 */
public class QueryRunner implements Runnable{
	public static int FETCH_SIZE = 100;
	
	private Connection con;
	private String query;
	private ResultConsumer<ResultSet> consumer;
	
	public QueryRunner(Connection con, String query, ResultConsumer<ResultSet> consumer) {
		this.con = con;
		this.query = query;
		this.consumer = consumer;
	}
	
	// time stamps
	public long started;
	public long finished;
	// rows processed
	public long rows = 0; 

	public void run(){
		started = System.currentTimeMillis();
		try(Statement stmt = createStatement()){
			stmt.setFetchSize(FETCH_SIZE);
			try(ResultSet rs = executeQuery(stmt)){
				consumer.onStart(new ResultSetInfo(rs.getMetaData()));
				while(rs.next()){
					rows++;
					consumer.accept(rs);
				}
				consumer.onFinish();
			}
		} catch (Exception e){
			consumer.onFailure(e);
			throw new RuntimeException(e);
		} finally{
			try {
				con.rollback(); // nothing to commit
			} catch (SQLException e) {
				// TODO log
			}
			finished = System.currentTimeMillis();
		}
	}
	
	protected Statement createStatement() throws SQLException{
		return con.createStatement();
	}
	protected ResultSet executeQuery(Statement stmt) throws SQLException{
		return stmt.executeQuery(query);
	}
	public Duration getDuration(){
		return Duration.ofMillis(finished - started);
	}
}
