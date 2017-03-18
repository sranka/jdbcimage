package pz.tool.jdbcimage.main;

import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;

import pz.tool.jdbcimage.ChunkedReader;
import pz.tool.jdbcimage.db.SqlExecuteCommand;
import pz.tool.jdbcimage.db.TableGroupedCommands;

/**
 * DB facade for PostgreSQL database.
 */
public class PostgreSQL extends DBFacade {
    public static final String STATE_TABLE_NAME = "jdbcimage_create_constraints";
    public static final String STATE_TABLE_DDL = "CREATE TABLE "+STATE_TABLE_NAME+"( tableName varchar(64),constraintName varchar(64),sql varchar(512))";
    public static final String STATE_TABLE_SQL = "SELECT tableName,constraintName,sql FROM "+STATE_TABLE_NAME+ " order by tableName,constraintName";
    private MainToolBase mainToolBase;

    private static Pattern identifyColumnPattern = Pattern.compile("^.*_([a-zA-z]*)_seq$");

    public PostgreSQL(MainToolBase mainToolBase) {
        this.mainToolBase = mainToolBase;
    }

    @Override
    public void setupDataSource(BasicDataSource bds) {
        bds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    @Override
    public List<String> getUserTables(Connection con) throws SQLException {
        List<String> retVal = new ArrayList<>();
        try(ResultSet tables = con.getMetaData().getTables(con.getCatalog(), con.getSchema(), "%", new String[]{"TABLE"})){
            while(tables.next()){
                String tableName = tables.getString(3);
                if (!STATE_TABLE_NAME.equalsIgnoreCase(tableName)) {
                    retVal.add(tableName);
                }
            }
        }
        return retVal;
    }

    @Override
    public Map<String, Object> getTablesWithIdentityColumn() {
        HashMap<String, Object> retVal = new HashMap<>();
        try (Connection con = mainToolBase.getReadOnlyConnection()) {
            try (Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(
                        "WITH fq_objects AS (SELECT c.oid,n.nspname AS nsp,c.relname AS name , \n" +
                        "                           c.relkind, c.relname AS relation \n" +
                        "                    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace ),\n" +
                        "     sequences AS (SELECT oid, nsp, name FROM fq_objects WHERE relkind = 'S'),  \n" +
                        "     tables    AS (SELECT oid, nsp, name FROM fq_objects WHERE relkind = 'r' )  \n" +
                        "SELECT\n" +
                        "       t.name AS tableName, \n" +
                        "       s.name AS sequence \n" +
                        "FROM \n" +
                        "     pg_depend d JOIN sequences s ON s.oid = d.objid  \n" +
                        "                 JOIN tables t ON t.oid = d.refobjid  \n" +
                        "WHERE \n" +
                        "     d.deptype = 'a' and t.nsp='"+currentSchema()+"'"
                        )){
                    while(rs.next()){
                        String tableName = rs.getString(1);
                        String sequence = rs.getString(2);
                        // extract column from generated name
                        Matcher matcher = identifyColumnPattern.matcher(sequence);
                        if (matcher.matches()){
                            String columnName = matcher.group(1);
                            retVal.put(tableName, new IdentityInfo(sequence, columnName));
                        }
                    }
                } finally {
                    con.rollback();
                }
            }
        }catch (SQLException e){
            throw new RuntimeException(e);
        }

        return retVal;
    }

    private static class IdentityInfo{
        String sequenceName;
        String columnName;

        IdentityInfo(String sequenceName, String columnName) {
            this.sequenceName = sequenceName;
            this.columnName = columnName;
        }
    }

    @Override
    public void afterImportTable(Connection con, String table, Object identityInfo) throws SQLException {
        if (identityInfo instanceof IdentityInfo) {
            IdentityInfo info = (IdentityInfo)identityInfo;
            String sql = "select setval('"+info.sequenceName+"', (select coalesce(max("+info.columnName+")+1,1) from "+table+"))";
            try (Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    rs.next();
                    Object val = rs.getLong(1); // read the next value of the sequence
                    System.out.println("Sequence "+info.sequenceName+" reset to "+val);
                }
            }
        }
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
            boolean createStateTable = false;
            try (Connection con = mainToolBase.getReadOnlyConnection()) {
                try (Statement stmt = con.createStatement()) {
                    try {
                        // check table existence
                        ResultSet rs = stmt.executeQuery("select 1 from " + STATE_TABLE_NAME);
                        rs.close();
                    } catch (SQLException e) {
                        // state table does not exist
                        createStateTable = true;
                    } finally {
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
                    "  where C.constraint_schema='" + currentSchema()+ "' order by S.table_name, C.constraint_name";
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

    @Override
    public int toSupportedSqlType(int sqlType) {
        switch (sqlType){
            // postgresql does not support unicode character types
            case Types.NCHAR: return Types.CHAR;
            case Types.NVARCHAR: return Types.VARCHAR;
            case Types.LONGNVARCHAR: return Types.LONGVARCHAR;
            // postgresql does not support BLOBs and CLOBs in the JDBC driver
            case Types.BLOB: return Types.VARBINARY;
            case Types.CLOB: return Types.LONGVARCHAR;
            case Types.NCLOB: return Types.LONGVARCHAR;
        }
        return sqlType;
    }

    @Override
    public boolean canCreateBlobs() {
        return false;
    }

    @Override
    public Object convertCharacterStreamInput(Reader reader) {
        if (reader instanceof ChunkedReader){
            return ((ChunkedReader)reader).readAsString();
        } else{
            // let the implementation handle this
            return reader;
        }
    }

    private volatile String cachedSchema = null;
    private String currentSchema() throws SQLException {
        if (cachedSchema == null) {
            // get current schema and create state table
            String schema;
            boolean createStateTable = false;
            try (Connection con = mainToolBase.getReadOnlyConnection()) {
                schema = con.getSchema(); // get current schema
            }
            this.cachedSchema = schema;
            return schema;
        } else{
            return cachedSchema;
        }
    }
}
