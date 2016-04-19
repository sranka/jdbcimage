package pz.tool.jdbcimage.main;

import org.apache.commons.dbcp2.BasicDataSource;
import pz.tool.jdbcimage.db.SqlExecuteCommand;
import pz.tool.jdbcimage.db.TableGroupedCommands;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DB facade for PostgreSQL database.
 */
public class PostgreSQL extends DBFacade {
    public static final String STATE_TABLE_NAME = "jdbcimage_create_constraints";
    public static final String STATE_TABLE_DDL = "CREATE TABLE "+STATE_TABLE_NAME+"( tableName varchar(64),constraintName varchar(64),sql varchar(512))";
    public static final String STATE_TABLE_SQL = "SELECT tableName,constraintName,sql FROM "+STATE_TABLE_NAME+ " order by tableName,constraintName";
    private MainToolBase mainToolBase;

    public PostgreSQL(MainToolBase mainToolBase) {
        this.mainToolBase = mainToolBase;
    }

    @Override
    public void setupDataSource(BasicDataSource bds) {
        bds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    @Override
    public ResultSet getUserTables(Connection con) throws SQLException {
        return con.getMetaData().getTables(con.getCatalog(), con.getSchema(), "%", new String[]{"TABLE"});
    }

    @Override
    public String escapeColumnName(String s) {
        return "\""+s+"\"";
    }

    @Override
    public String escapeTableName(String s) {
        return "\""+s+"\"";
    }

    @Override
    public void modifyConstraints(boolean enable) throws SQLException {
        final TableGroupedCommands commands = new TableGroupedCommands();
        if (!enable) {
            // get current schema and create state table
            String schema;
            boolean createStateTable = false;
            try (Connection con = mainToolBase.getReadOnlyConnection()) {
                schema = con.getSchema(); // get current schema
                try(Statement stmt = con.createStatement()){
                    try{
                        // check table existence
                        ResultSet rs = stmt.executeQuery("select 1 from "+STATE_TABLE_NAME);
                        rs.close();
                    } catch(SQLException e){
                        // state table does not exist
                        createStateTable = true;
                    } finally{
                        con.rollback();
                    }
                }
            }
            if (createStateTable){
                try (Connection con = mainToolBase.getWriteConnection()) {
                    try(Statement stmt = con.createStatement()){
                        stmt.execute(STATE_TABLE_DDL);
                        con.commit();
                    } catch(Exception e){
                        con.rollback();
                    }
                }
            }

            // reconstruct current foreign key constraints
            String query = "select \n" +
                    "  C.constraint_name\n" +
                    "  ,S.table_name, S.column_name\n" +
                    "  ,T.table_name, T.column_name\n" +
                    "  ,C.match_option,C.update_rule,C.delete_rule\n" +
                    "  ,C2.is_deferrable, C2.initially_deferred\n" +
                    "  from information_schema.referential_constraints C \n" +
                    "  join information_schema.table_constraints C2 on (C.constraint_schema = C2.constraint_schema and C.constraint_name = C2.constraint_name) \n" +
                    "  join information_schema.key_column_usage S on (S.constraint_schema = C.constraint_schema and S.constraint_name = C.constraint_name) \n" +
                    "  join information_schema.constraint_column_usage T on (T.constraint_schema = C.unique_constraint_schema and T.constraint_name = C.unique_constraint_name) \n" +
                    "  where C.constraint_schema='" + schema + "' order by S.table_name, C.constraint_name";
            Map<String,String> constraints = new HashMap<>(); // check for duplicates
            mainToolBase.executeQuery(
                    query,
                    row -> {
                        try {
                            String constraint = row.getString(1);
                            String tableName = row.getString(2);
                            if (constraints.put(constraint,constraint)!=null){
                                throw new UnsupportedOperationException("The implementation does not support foreign key reference groups!" +
                                        "Unsupported constraint '"+constraint+"' defined on table '"+tableName+"'");
                            }
                            if (mainToolBase.containsTable(tableName) && !tableName.equals(STATE_TABLE_NAME)) {
                                // drop commands
                                String desc = "Drop constraint " + constraint + " on table " + tableName;
                                String sql = "ALTER TABLE \"" + tableName + "\" DROP CONSTRAINT \"" + constraint + "\"";
                                commands.add(tableName, desc, sql);
                                // reconstruct the way of how to create the constraint
                                String sourceColumn = row.getString(3);
                                String targetTable = row.getString(4);
                                String targetColumn = row.getString(5);
                                String match_option = row.getString(6);
                                String update_rule = row.getString(7);
                                String delete_rule = row.getString(8);
                                boolean is_deferrable = "YES".equals(row.getString(9));
                                boolean initially_deferred = "YES".equals(row.getString(10));
                                StringBuilder createSql = new StringBuilder();
                                createSql.append("ALTER TABLE ").append(tableName);
                                createSql.append(" ADD CONSTRAINT ").append(constraint);
                                createSql.append(" FOREIGN KEY (").append(sourceColumn).append(")");
                                createSql.append(" REFERENCES ").append(targetTable);
                                createSql.append(" (").append(targetColumn).append(")");
                                if (!"NONE".equals(match_option)) createSql.append(" MATCH ").append(match_option);
                                if (!"NO ACTION".equals(update_rule)) createSql.append(" ON UPDATE ").append(update_rule);
                                if (!"NO ACTION".equals(delete_rule)) createSql.append(" ON DELETE ").append(delete_rule);
                                if (is_deferrable) createSql.append(" DEFERRABLE");
                                if (initially_deferred) createSql.append(" INITIALLY DEFERRED");
                                desc = "Persist DDL for constraint " + constraint + " on table " + tableName;
                                sql = "INSERT INTO "+STATE_TABLE_NAME+"(tableName,constraintName,sql) " +
                                        "VALUES('"+tableName+"','"+constraint+"','"+createSql.toString()+"')";
                                commands.add(tableName, desc, sql);
                                return null;
                            } else {
                                return null;
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        } else{
            // read commands from the database and delete them on the fly
            mainToolBase.executeQuery(
                    STATE_TABLE_SQL,
                    row -> {
                        try{
                            String tableName = row.getString(1);
                            String constraint = row.getString(2);
                            String sql = row.getString(3);

                            // create constraint
                            String desc = "Create constraint " + constraint + " on table " + tableName;
                            commands.add(tableName, desc, sql);
                            // create constraint
                            desc = "Delete persisted DDL for constraint " + constraint + " on table " + tableName;
                            sql = "DELETE FROM "+STATE_TABLE_NAME+" WHERE" +
                                    " tableName='"+tableName+"' AND constraintName='"+constraint+"'";
                            commands.add(tableName, desc, sql);

                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
            );
        }
        mainToolBase.runSerial(commands.tableGroups
                .stream()
                .map(x -> SqlExecuteCommand.toSqlExecuteTask(
                        mainToolBase.getWriteConnectionSupplier(),
                        mainToolBase.out,
                        x.toArray(new SqlExecuteCommand[x.size()]))
                )
                .collect(Collectors.toList()));
    }

    @Override
    public void modifyIndexes(boolean enable) throws SQLException {
        mainToolBase.out.println("Index " + (enable ? "enable" : "disable") + " not supported on PostgreSQL!");
    }
}
