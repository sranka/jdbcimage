package pz.tool.jdbcimage.main;

import org.apache.commons.dbcp2.BasicDataSource;
import pz.tool.jdbcimage.db.SqlExecuteCommand;
import pz.tool.jdbcimage.db.TableGroupedCommands;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DB facade for Oracle database.
 */
public class Oracle extends DBFacade {
    private MainToolBase mainToolBase;

    public Oracle(MainToolBase mainToolBase) {
        this.mainToolBase = mainToolBase;
    }

    @Override
    public void setupDataSource(BasicDataSource bds) {
        List<String> connectionInits = Arrays.asList(
                "ALTER SESSION ENABLE PARALLEL DDL", //could possibly make index disabling quicker
                "ALTER SESSION SET skip_unusable_indexes = TRUE" //avoid ORA errors caused by unusable indexes
        );
        bds.setConnectionInitSqls(connectionInits);
        // the minimum level supported by Oracle
        bds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Override
    public List<String> getUserTables(Connection con) throws SQLException {
        // return tables that are not materialized views as well
        // exclude materialized views
        Map<String,Boolean> toExclude = new HashMap<>();
        mainToolBase.executeQuery(
                "SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE='MATERIALIZED VIEW'",
                row ->{
                    try {
                        toExclude.put(row.getString(1),true);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
        );
        // include tables
        return mainToolBase.executeQuery(
                "SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE='TABLE'",
                row ->{
                    try {
                        String tableName = row.getString(1);
                        if (toExclude.containsKey(tableName)){
                            return null;
                        } else{
                            return tableName;
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    @Override
    public String escapeColumnName(String s) {
        return "\"" + s + "\"";
    }

    @Override
    public String escapeTableName(String s) {
        return "\"" + s + "\"";
    }

    @Override
    public void modifyConstraints(boolean enable) throws SQLException {
        String[] conditions;
        if (enable) {
            // on enable: enable foreign indexes after other types
            conditions = new String[]{"CONSTRAINT_TYPE<>'R'", "CONSTRAINT_TYPE='R'"};
        } else {
            // on disable: disable foreign indexes first
            conditions = new String[]{"CONSTRAINT_TYPE='R'", "CONSTRAINT_TYPE<>'R'"};
        }
        TableGroupedCommands commands = new TableGroupedCommands();
        for (int i = 0; i < 2; i++) {
            mainToolBase.executeQuery(
                    "SELECT OWNER,TABLE_NAME,CONSTRAINT_NAME FROM user_constraints WHERE " + conditions[i] + " order by TABLE_NAME",
                    row -> {
                        try {
                            String owner = row.getString(1);
                            String tableName = row.getString(2);
                            String constraint = row.getString(3);
                            if (mainToolBase.containsTable(tableName)) {
                                if (enable) {
                                    String desc = "Enable constraint " + constraint + " on table " + tableName;
                                    String sql = "ALTER TABLE " + owner + "." + tableName
                                            + " MODIFY CONSTRAINT " + constraint + " ENABLE";
                                    commands.add(tableName, desc, sql);
                                } else {
                                    String desc = "Disable constraint " + constraint + " on table " + tableName;
                                    String sql = "ALTER TABLE " + owner + "." + tableName
                                            + " MODIFY CONSTRAINT " + constraint + " DISABLE";
                                    commands.add(tableName, desc, sql);
                                }
                                return null;
                            } else {
                                return null;
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
            mainToolBase.run(commands.tableGroups
                    .stream()
                    .map(x -> SqlExecuteCommand.toSqlExecuteTask(
                            mainToolBase.getWriteConnectionSupplier(),
                            mainToolBase.out,
                            x.toArray(new SqlExecuteCommand[x.size()]))
                    )
                    .collect(Collectors.toList()));
        }
    }

    @Override
    public void modifyIndexes(boolean enable) throws SQLException {
        TableGroupedCommands commands = new TableGroupedCommands();
        mainToolBase.executeQuery(
                /** exclude LOB indexes, since they cannot be altered */
                "SELECT TABLE_OWNER,TABLE_NAME,INDEX_NAME FROM user_indexes where INDEX_TYPE<>'LOB' order by TABLE_NAME",
                row -> {
                    try {
                        String owner = row.getString(1);
                        String tableName = row.getString(2);
                        String index = row.getString(3);
                        if (mainToolBase.containsTable(tableName)) {
                            if (enable) {
                                String desc = "Rebuild index " + index + " on table " + tableName;
                                String sql = "ALTER INDEX " + owner + "." + index
                                        // SHOULD BE "REBUILD ONLINE" ... but it works only on Enterprise Edition on oracle
                                        + " REBUILD";
                                commands.add(tableName, desc, sql);
                            } else {
                                String desc = "Disable index " + index + " on table " + tableName;
                                String sql = "ALTER INDEX " + owner + "." + index
                                        + " UNUSABLE";
                                commands.add(tableName, desc, sql);
                            }
                            return null;
                        } else {
                            return null;
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
        mainToolBase.run(commands.tableGroups
                .stream()
                .map(x -> SqlExecuteCommand.toSqlExecuteTask(
                        mainToolBase.getWriteConnectionSupplier(),
                        mainToolBase.out,
                        x.toArray(new SqlExecuteCommand[x.size()]))
                )
                .collect(Collectors.toList()));
    }
}
