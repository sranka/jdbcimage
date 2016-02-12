package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * DB import that runs in multiple threads.
 */
public class MultiTableParallelImport extends SingleTableImport{
	
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
	
	/**
	 * Main execution point.
	 */
	public void run() throws SQLException, IOException{
		// print platform parallelism, just FYI
		out.println("-- Parallelism "+ parallelism);

		Durations durations = new Durations();
		try {
			List<String> dbTables = getUserTables();
			Map<String,String> dbTablesMap = new HashMap<>();
			dbTables.stream().forEach(x -> dbTablesMap.put(x.toLowerCase(),x));
			// collect tables to import (ignore tables that do not exist)
			setTables(Files.list(Paths.get(tool_builddir))
				.filter(x -> {
					File f = x.toFile();
					return f.isFile() && !f.getName().contains(".");
				}).map(x -> x.getFileName().toString())
				.filter(x -> dbTablesMap.containsKey(x.toLowerCase()))
				.collect(Collectors.toList()));
			if (tables.size() != 0){
				// apply a procedure that ignores indexes and constraints 
				// to speed up data import
				
				// 1. disable constraints
				durations.disableConstraints = dbFacade.modifyConstraints(false);
				// 2. make indexes unusable
				if (tool_disableIndexes) durations.disableIndexes = dbFacade.modifyIndexes(false);
				// 3. delete data
				durations.deleteData = deleteData();
				// 4. do import
				durations.importData = importData();
				// 5. rebuild indexes
				if (tool_disableIndexes) durations.enableIndexes = dbFacade.modifyIndexes(true);
				// 6. enable constraints
				durations.enableConstraints = dbFacade.modifyConstraints(true);
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
		Set<String> tablesWithIdentityColumns = dbFacade.getTablesWithIdentityColumns();
		for(String table: tables){
			tasks.add(new Callable<Void>(){
				@Override
				public Void call() throws Exception {
					boolean failed = true;
					try{
						Duration time = importTable(table, tablesWithIdentityColumns.contains(table));
						out.println("SUCCESS: Imported data to "+table+" in "+time);
						failed = false;
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

	public static void main(String... args) throws Exception{
		try(MultiTableParallelImport tool = new MultiTableParallelImport()){tool.run();}
	}
}
