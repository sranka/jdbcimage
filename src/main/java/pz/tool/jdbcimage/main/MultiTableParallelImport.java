package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DB import that runs in multiple threads.
 */
public class MultiTableParallelImport extends SingleTableImport{
	
	// tables to import
	private List<String> tables = null;
	private Map<String,String> tableSet = null;
	
	/**
	 * Durations to print out at the end.
	 */
	private static class Durations{
		Duration disableConstraints = null;
		Duration disableIndexes = null;
		Duration enableConstraints = null;
		Duration enableIndexes = null;
		Duration deleteData = null;
		Duration importData = null;
	}
	
	private void setTables(List<String> tables){
		this.tables = tables;
		this.tableSet = new HashMap<>();
		for(String t: tables){
			tableSet.put(t.toLowerCase(), t);
		}
	}
	private boolean containsTable(String table){
		return tableSet.containsKey(table.toLowerCase());
	}
	
	/**
	 * Main execution point.
	 */
	public void run() throws SQLException, IOException{
		// print platform parallelism, just FYI
		out.println("-- Parallelism "+ parallelism);

		Durations durations = new Durations();
		try {
			// collect tables to import
			// TODO ignore tables that do not exist
			setTables(Files.list(Paths.get(tool_builddir))
				.filter(x -> {
					File f = x.toFile();
					return f.isFile() && !f.getName().contains(".");
				}).map(x -> x.getFileName().toString())
				.collect(Collectors.toList()));
			if (tables.size() != 0){
				// apply a procedure that ignores indexes and constraints 
				// to speed up data import
				
				// 1. disable constraints
				durations.disableConstraints = modifyConstraints(false);
				// 2. make indexes unusable
				if (tool_disableIndexes) durations.disableIndexes = modifyIndexes(false);
				// 3. delete data
				durations.deleteData = deleteData();
				// 4. do import
				durations.importData = importData();
				// 5. rebuild indexes
				if (tool_disableIndexes) durations.enableIndexes = modifyIndexes(true);
				// 6. enable constraints
				durations.enableConstraints = modifyConstraints(true);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		out.println("Disable contrainsts time: "+ durations.disableConstraints);
		out.println("Disable indexes time: "+ durations.disableIndexes);
		out.println("Delete data time: "+ durations.deleteData);
		out.println("Import data time: "+ durations.importData);
		out.println("Enable indexes time: "+ durations.enableIndexes);
		out.println("Enable contraints time: "+ durations.enableConstraints);
	}
	
	/**
	 * Described SQL Command to execute. 
	 */
	public static class SqlExecuteCommand{
		public String description;
		public String sql;

		public SqlExecuteCommand(String description, String sql) {
			this.description = description;
			this.sql = sql;
		}
	}
	
	/**
	 * IndexCommand groups index modifications by table name
	 * so that each group can be run in parallel.
	 */ 
	public static class TableGroupedCommands{
		public List<List<SqlExecuteCommand>> tableGroups = new ArrayList<>();
		private List<SqlExecuteCommand> lastGroup = null;
		private String lastTable = null;
		
		public void add(String table, String description, String sql){
			if (!table.equals(lastTable)){
				lastTable = table;
				lastGroup = new ArrayList<>();
				tableGroups.add(lastGroup);
			}
			lastGroup.add(new SqlExecuteCommand(description,sql));
		}
	}

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
					.map(x -> toSqlExecuteTask(x.toArray(new SqlExecuteCommand[x.size()])))
					.collect(Collectors.toList()));
		}
		return Duration.ofMillis(System.currentTimeMillis()-time);
	}

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
				.map(x -> toSqlExecuteTask(x.toArray(new SqlExecuteCommand[x.size()])))
				.collect(Collectors.toList()));
		return Duration.ofMillis(System.currentTimeMillis()-time);
	}

	public Duration deleteData(){
		long time = System.currentTimeMillis();
		List<Callable<?>> tasks = new ArrayList<>(tables.size());
		for(String table: tables){
			tasks.add(new Callable<Void>(){
				@Override
				public Void call() throws Exception {
					boolean failed = true;
					try{
						truncateTable(table);
						out.println("SUCCESS: Truncated table "+table);
						failed = false;
						// TODO handle the case of a missing table properly
					} finally{
						if (failed){
							out.println("FAILURE: Truncate table "+table);
						}
					}
					return null;
				}
			});
		}
		run(tasks);
		return Duration.ofMillis(System.currentTimeMillis()-time);
	}

	public Duration importData(){
		long time = System.currentTimeMillis();
		List<Callable<?>> tasks = new ArrayList<>(tables.size());
		for(String table: tables){
			tasks.add(new Callable<Void>(){
				@Override
				public Void call() throws Exception {
					boolean failed = true;
					try{
						Duration time = importTable(table);
						out.println("SUCCESS: Imported data to "+table+" in "+time);
						failed = false;
						// TODO handle the case of a missing table properly
					} finally{
						if (failed){
							out.println("FAILURE: Import data to table "+table);
						}
					}
					return null;
				}
			});
		}
		run(tasks);
		return Duration.ofMillis(System.currentTimeMillis()-time);
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
					// TODO log
				}
			}
			return retVal;
		}
	}
	
	public Callable<Void> toSqlExecuteTask(SqlExecuteCommand... commands){
		return new Callable<Void>(){
			@Override
			public Void call() throws Exception {
				try(Connection con = getWriteConnection()){
					boolean failed = true;
					SqlExecuteCommand last = null;
					try(Statement stmt = con.createStatement()){
						for(SqlExecuteCommand cmd: commands){
							last = cmd;
							stmt.execute(last.sql);
							out.println("SUCCESS: "+last.description);
						}
						failed = false;
					} finally{
						try {
							if (failed) {
								if (last!=null) out.println("FAILURE: "+last.description);
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
    
	public static void main(String... args) throws Exception{
		try(MultiTableParallelImport tool = new MultiTableParallelImport()){tool.run();}
	}
}
