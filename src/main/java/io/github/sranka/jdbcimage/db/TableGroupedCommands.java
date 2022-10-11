package io.github.sranka.jdbcimage.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Groups commands by table name so that groups can be run concurrently
 * to avoid database deadlocks.
 */ 
public class TableGroupedCommands{
    public List<List<SqlExecuteCommand>> tableGroups = new ArrayList<>();
    private List<SqlExecuteCommand> lastGroup = null;
    private String lastTable = null;
    private boolean empty = true;

    public void add(String table, String description, String sql){
        if (!table.equals(lastTable)){
            lastTable = table;
            lastGroup = new ArrayList<>();
            tableGroups.add(lastGroup);
        }
        empty = false;
        lastGroup.add(new SqlExecuteCommand(description,sql));
    }

    public boolean isEmpty() {
        return empty;
    }
}