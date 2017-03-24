package pz.tool.jdbcimage.main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;

import pz.tool.jdbcimage.LoggedUtils;
import pz.tool.jdbcimage.db.SqlExecuteCommand;
import pz.tool.jdbcimage.db.TableGroupedCommands;

/**
 * DB facade for MSSQL database.
 */
@SuppressWarnings({"WeakerAccess", "SqlNoDataSourceInspection", "SqlDialectInspection"})
public class Mssql extends DBFacade {
    private MainToolBase mainToolBase;

    public Mssql(MainToolBase mainToolBase) {
        setToolBase(mainToolBase);
    }

    @Override
    public void setToolBase(MainToolBase mainToolBase) {
        this.mainToolBase = mainToolBase;
    }

    @Override
    public void setupDataSource(BasicDataSource bds) {
        bds.setDefaultTransactionIsolation(Connection.TRANSACTION_NONE);
    }

    @Override
    public List<String> getDbUserTables(Connection con) throws SQLException {
        List<String> retVal = new ArrayList<>();
        try(ResultSet tables = con.getMetaData().getTables(con.getCatalog(), "dbo", "%", new String[]{"TABLE"})){
            while(tables.next()){
                String tableName = tables.getString(3);
                retVal.add(tableName);
            }
        }
        return retVal;
    }

    @Override
    public String escapeColumnName(String s) {
        return "[" + s + "]";
    }

    @Override
    public String escapeTableName(String s) {
        return "[" + s + "]";
    }

    @Override
    public void modifyConstraints(boolean enable) throws SQLException {
        List<String> queries = new ArrayList<>();
        // table name, foreign key name
        queries.add("SELECT t.Name, dc.Name "
                + "FROM sys.tables t INNER JOIN sys.foreign_keys dc ON t.object_id = dc.parent_object_id "
                + "ORDER BY t.Name");
        TableGroupedCommands commands = new TableGroupedCommands();
        for (String query : queries) {
            mainToolBase.executeQuery(
                    query,
                    row -> {
                        try {
                            String tableName = row.getString(1);
                            String constraint = row.getString(2);
                            if (mainToolBase.containsTable(tableName)) {
                                if (enable) {
                                    String desc = "Enable constraint " + constraint + " on table " + tableName;
                                    String sql = "ALTER TABLE [" + tableName + "] CHECK CONSTRAINT [" + constraint + "]";
                                    commands.add(tableName, desc, sql);
                                } else {
                                    String desc = "Disable constraint " + constraint + " on table " + tableName;
                                    String sql = "ALTER TABLE [" + tableName + "] NOCHECK CONSTRAINT [" + constraint + "]";
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
            // there are DEADLOCK problems when running in parallel
            mainToolBase.runSerial(commands.tableGroups
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
        mainToolBase.out.println("Index " + (enable ? "enable" : "disable") + " not supported on MSSQL!");
    }

    @Override
    public String getTruncateTableSql(String tableName) {
        // unable to use TRUNCATE TABLE on MSSQL server even with CONSTRAINTS DISABLED!
        return "DELETE FROM " + escapeTableName(tableName);
    }

    @Override
    public void afterImportTable(Connection con, String table, TableInfo tableInfo) throws SQLException {
        super.afterImportTable(con, table, tableInfo);
        if (tableInfo.get("hasId")!=null) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute("SET IDENTITY_INSERT [" + table + "] OFF");
            }
        }
    }

    @Override
    public void beforeImportTable(Connection con, String table, TableInfo tableInfo) throws SQLException {
        super.beforeImportTable(con, table, tableInfo);
        if (tableInfo.get("hasId")!=null) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute("SET IDENTITY_INSERT [" + table + "] ON");
            }
        }
    }

    private Map<String, Boolean> hasIdentityColumns = null;

    @Override
    public void importStarted() {
        Map<String, Boolean> retVal = new HashMap<>();
        try (Connection con = mainToolBase.getReadOnlyConnection()) {
            try (Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(
                        "select name from sys.objects where type = 'U' and OBJECTPROPERTY(object_id, 'TableHasIdentity')=1")) {
                    while (rs.next()) {
                        retVal.put(rs.getString(1), Boolean.TRUE);
                    }
                }
            } finally {
                try {
                    con.rollback(); // nothing to commit
                } catch (SQLException e) {
                    LoggedUtils.ignore("Unable to rollback!", e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        hasIdentityColumns = retVal;
    }
    @Override
    public TableInfo getTableInfo(String tableName) {
        TableInfo retVal = super.getTableInfo(tableName);
        retVal.put("hasId", hasIdentityColumns.get(tableName));

        return retVal;
    }

}
