package pz.tool.jdbcimage.main;

import org.apache.commons.dbcp2.BasicDataSource;
import pz.tool.jdbcimage.db.SqlExecuteCommand;
import pz.tool.jdbcimage.db.TableGroupedCommands;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
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
    public ResultSet getUserTables(Connection con) throws SQLException {
        return con.getMetaData().getTables(con.getCatalog(), mainToolBase.jdbc_user.toUpperCase(), "%", new String[]{"TABLE"});
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
    public Duration modifyConstraints(boolean enable) throws SQLException {
        long time = System.currentTimeMillis();
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
        return Duration.ofMillis(System.currentTimeMillis() - time);
    }

    @Override
    public Duration modifyIndexes(boolean enable) throws SQLException {
        long time = System.currentTimeMillis();
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
        return Duration.ofMillis(System.currentTimeMillis() - time);
    }
}
