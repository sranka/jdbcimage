package pz.tool.jdbcimage.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Groups commands by table name so that groups can be run in parallel 
 * to avoid database deadlocks.
 */ 
public class TableGroupedCommands{
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