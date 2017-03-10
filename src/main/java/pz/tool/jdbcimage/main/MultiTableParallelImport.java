package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
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
		// print platform concurrency, just FYI
		out.println("Concurrency: "+ concurrency);
		unzip(); // unzip input if it exists
		
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
				.map(x -> {
					String retVal = dbTablesMap.get(x.toLowerCase());
					if (retVal == null){
						out.println("SKIPPED - table "+x+" does not exists!");
					}
					return retVal;
				})
				.filter(x -> x!=null)
				.collect(Collectors.toList()));
			if (tables.size() != 0){
				// apply a procedure that ignores indexes and constraints 
				// to speed up data import

				long time;
				// 1. disable constraints
				time = System.currentTimeMillis();
				dbFacade.modifyConstraints(false);
				durations.disableConstraints = Duration.ofMillis(System.currentTimeMillis() - time);
				// 2. make indexes unusable skipped
				if (tool_disableIndexes){
					time = System.currentTimeMillis();
					dbFacade.modifyIndexes(false);
					durations.disableIndexes = Duration.ofMillis(System.currentTimeMillis() - time);
				}
				// 3. delete data
				time = System.currentTimeMillis();
				deleteData();
				durations.deleteData = Duration.ofMillis(System.currentTimeMillis() - time);
				// 4. do import
				time = System.currentTimeMillis();
				importData();
				durations.importData = Duration.ofMillis(System.currentTimeMillis() - time);
				// 5. rebuild indexes
				if (tool_disableIndexes){
					time = System.currentTimeMillis();
					dbFacade.modifyIndexes(true);
					durations.enableIndexes = Duration.ofMillis(System.currentTimeMillis() - time);
				}
				// 6. enable constraints
				time = System.currentTimeMillis();
				dbFacade.modifyConstraints(true);
				durations.enableConstraints  = Duration.ofMillis(System.currentTimeMillis() - time);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		out.println("Disable constraints time: "+ durations.disableConstraints);
		if (tool_disableIndexes) out.println("Disable indexes time: "+ durations.disableIndexes);
		out.println("Delete data time: "+ durations.deleteData);
		out.println("Import data time: "+ durations.importData);
		if (tool_disableIndexes) out.println("Enable indexes time: "+ durations.enableIndexes);
		out.println("Enable constraints time: "+ durations.enableConstraints);
	}
	
	public Duration deleteData(){
		long time = System.currentTimeMillis();
		List<Callable<?>> tasks = new ArrayList<>(tables.size());
		for(String table: tables){
			tasks.add(() -> {
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
			tasks.add(() -> {
				boolean failed = true;
				try{
					importTable(table, tablesWithIdentityColumns.contains(table));
					out.println("SUCCESS: Imported data to "+table+" in "+Duration.ofMillis(System.currentTimeMillis()-time));
					failed = false;
				} finally{
					if (failed){
						out.println("FAILURE: Import data to table "+table);
					}
				}
				return null;
			});
		}
		run(tasks);
		return Duration.ofMillis(System.currentTimeMillis()-time);
	}

	public static void main(String... args) throws Exception{
		try(MultiTableParallelImport tool = new MultiTableParallelImport()){
			tool.setupZipFile(args);
			tool.run();
		} 
	}
}