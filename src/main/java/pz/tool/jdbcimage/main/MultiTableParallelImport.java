package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

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
			dbTables.forEach(x -> dbTablesMap.put(x.toLowerCase(),x));
			Map<String, String> conflictingFiles = new HashMap<>();
			// collect tables to import (ignore tables that do not exist)
			setTables(Files.list(Paths.get(tool_builddir))
				.filter(x -> {
					File f = x.toFile();
					return f.isFile() && !f.getName().contains(".");
				})
				.collect(
						LinkedHashMap<String,String>::new,
						(map, x) -> {
							String fileName = x.getFileName().toString();
							String lowerCaseTableName = fileName.toLowerCase();
							String retVal = dbTablesMap.get(lowerCaseTableName);
							if (retVal == null){
								out.println("SKIPPED - table "+x+" does not exists!");
							} else{
								map.put(retVal, fileName);
							}
							String previousFile = conflictingFiles.put(lowerCaseTableName, fileName);
							if (previousFile != null){
								throw new RuntimeException("Unsupported data on input. Only one files must describe a case-sensitive table, but found "+previousFile+" and "+fileName);
							}
						},
						LinkedHashMap::putAll
				), out);
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
		for(String table: tables.keySet()){
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
		Map<String, ?> tablesWithIdentityColumns = dbFacade.getTablesWithIdentityColumn();
		for(Map.Entry<String,String> entry: tables.entrySet()){
			String table = entry.getKey();
			String fileName = entry.getValue();
			tasks.add(() -> {
				boolean failed = true;
				try{
					long start = System.currentTimeMillis();
					long rows = importTable(table, fileName, tablesWithIdentityColumns.get(table));
					out.println("SUCCESS: Imported data to "+table+" - "+rows+" rows in "+Duration.ofMillis(System.currentTimeMillis()-start));
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