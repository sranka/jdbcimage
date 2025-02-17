package io.github.sranka.jdbcimage.main;

import io.github.sranka.jdbcimage.ChunkedReader;
import io.github.sranka.jdbcimage.db.SqlExecuteCommand;
import io.github.sranka.jdbcimage.db.TableGroupedCommands;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * DB facade for PostgreSQL database.
 */
public class PostgreSQL extends DBFacade {
    public static final String STATE_TABLE_NAME = "jdbcimage_create_constraints";
    public static final String STATE_TABLE_DDL = "CREATE TABLE " + STATE_TABLE_NAME + "( tableName varchar(64),constraintName varchar(64),sql varchar(512))";
    public static final String STATE_TABLE_SQL = "SELECT tableName,constraintName,sql FROM " + STATE_TABLE_NAME + " order by tableName,constraintName";

    private static final Pattern identifyColumnPattern = Pattern.compile("^.*_([a-zA-z]*)_seq$");
    private Map<String, IdentityInfo> allIdentityInfo = null;
    private volatile String cachedSchema = null;

    @Override
    public void setupDataSource(BasicDataSource bds) {
        bds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    @Override
    public boolean isTableIgnored(String tableName) {
        return STATE_TABLE_NAME.equalsIgnoreCase(tableName) || super.isTableIgnored(tableName);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public List<String> getDbUserTables(Connection con) throws SQLException {
        List<String> retVal = new ArrayList<>();
        try (ResultSet tables = con.getMetaData().getTables(con.getCatalog(), con.getSchema(), "%", new String[]{"TABLE"})) {
            while (tables.next()) {
                String tableName = tables.getString(3);
                retVal.add(tableName);
            }
        }
        return retVal;
    }

    @Override
    public void importStarted() {
        HashMap<String, IdentityInfo> retVal = new HashMap<>();
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
                                "     d.deptype = 'a' and t.nsp='" + currentSchema() + "'"
                )) {
                    while (rs.next()) {
                        String tableName = rs.getString(1);
                        String sequence = rs.getString(2);
                        // extract column from generated name
                        Matcher matcher = identifyColumnPattern.matcher(sequence);
                        if (matcher.matches()) {
                            String columnName = matcher.group(1);
                            retVal.put(tableName, new IdentityInfo(sequence, columnName));
                        }
                    }
                } finally {
                    con.rollback();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        allIdentityInfo = retVal;
    }

    @Override
    public TableInfo getTableInfo(String tableName) {
        TableInfo retVal = super.getTableInfo(tableName);
        retVal.put("IdInfo", allIdentityInfo.get(tableName));

        return retVal;
    }

    @Override
    public void afterImportTable(Connection con, String table, TableInfo tableInfo) throws SQLException {
        super.afterImportTable(con, table, tableInfo);
        IdentityInfo info = (IdentityInfo) tableInfo.get("IdInfo");
        if (info != null) {
            String sql = "select setval('" + info.sequenceName + "', (select coalesce(max(" + info.columnName + ")+1,1) from " + table + "))";
            try (Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    rs.next();
                    Object val = rs.getLong(1); // read the next value of the sequence
                    Env.out.println("Sequence " + info.sequenceName + " reset to " + val);
                }
            }
        }
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
        final TableGroupedCommands commands = new TableGroupedCommands();
        if (!enable) {
            if (requiresCreateStateTable()) {
                try (Connection con = mainToolBase.getWriteConnection()) {
                    try (Statement stmt = con.createStatement()) {
                        stmt.execute(STATE_TABLE_DDL);
                        con.commit();
                    } catch (Exception e) {
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
                    "  join information_schema.key_column_usage T on (T.constraint_schema = C.unique_constraint_schema and T.constraint_name = C.unique_constraint_name and S.ordinal_position = T.ordinal_position) \n" +
                    "  where C.constraint_schema='" + currentSchema() + "' order by S.table_name, C.constraint_name, S.ordinal_position";
            Map<String, ForeignKeyConstraint> constraints = new LinkedHashMap<>();
            mainToolBase.executeQuery(
                    query,
                    row -> {
                        try {
                            String constraint = row.getString(1);
                            String tableName = row.getString(2);
                            if (mainToolBase.containsTable(tableName) && !tableName.equals(STATE_TABLE_NAME)) {
                                String sourceColumn = row.getString(3);
                                String targetTable = row.getString(4);
                                String targetColumn = row.getString(5);

                                ForeignKeyConstraint data;
                                if (!constraints.containsKey(constraint)) {
                                    String match_option = row.getString(6);
                                    String update_rule = row.getString(7);
                                    String delete_rule = row.getString(8);
                                    boolean is_deferrable = "YES".equals(row.getString(9));
                                    boolean initially_deferred = "YES".equals(row.getString(10));
                                    data = new ForeignKeyConstraint(constraint, tableName, targetTable, new ArrayList<>(), match_option, update_rule, delete_rule, is_deferrable, initially_deferred);
                                    constraints.put(constraint, data);
                                } else {
                                    data = constraints.get(constraint);
                                }
                                data.columns.add(new SourceTargetColumn(sourceColumn, targetColumn));
                            }
                            return null;
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
            );
            constraints.forEach((constraint, data) -> {
                String tableName = data.tableName;

                // drop commands
                String desc = "Drop constraint " + constraint + " on table " + tableName;
                String sql = "ALTER TABLE \"" + tableName + "\" DROP CONSTRAINT \"" + constraint + "\"";
                commands.add(tableName, desc, sql);
                // reconstruct the way of how to create the constraint
                String targetTable = data.targetTable;
                String sourceColumns = data.columns.stream().map(c -> c.source).collect(Collectors.joining(","));
                String targetColumns = data.columns.stream().map(c -> c.target).collect(Collectors.joining(","));
                String match_option = data.match_option;
                String update_rule = data.update_rule;
                String delete_rule = data.delete_rule;
                boolean is_deferrable = data.is_deferrable;
                boolean initially_deferred = data.initially_deferred;
                StringBuilder createSql = new StringBuilder();
                createSql.append("ALTER TABLE ").append(tableName);
                createSql.append(" ADD CONSTRAINT ").append(constraint);
                createSql.append(" FOREIGN KEY (").append(sourceColumns).append(")");
                createSql.append(" REFERENCES ").append(targetTable);
                createSql.append(" (").append(targetColumns).append(")");
                if (!"NONE".equals(match_option)) createSql.append(" MATCH ").append(match_option);
                if (!"NO ACTION".equals(update_rule)) createSql.append(" ON UPDATE ").append(update_rule);
                if (!"NO ACTION".equals(delete_rule)) createSql.append(" ON DELETE ").append(delete_rule);
                if (is_deferrable) createSql.append(" DEFERRABLE");
                if (initially_deferred) createSql.append(" INITIALLY DEFERRED");
                desc = "Persist DDL for constraint " + constraint + " on table " + tableName;
                sql = "INSERT INTO " + STATE_TABLE_NAME + "(tableName,constraintName,sql) " +
                        "VALUES('" + tableName + "','" + constraint + "','" + createSql + "')";
                commands.add(tableName, desc, sql);
            });
        } else {
            // read commands from the database and delete them on the fly
            mainToolBase.executeQuery(
                    STATE_TABLE_SQL,
                    row -> {
                        try {
                            String tableName = row.getString(1);
                            String constraint = row.getString(2);
                            String sql = row.getString(3);

                            // create constraint
                            String desc = "Create constraint " + constraint + " on table " + tableName;
                            commands.add(tableName, desc, sql);
                            // create constraint
                            desc = "Delete persisted DDL for constraint " + constraint + " on table " + tableName;
                            sql = "DELETE FROM " + STATE_TABLE_NAME + " WHERE" +
                                    " tableName='" + tableName + "' AND constraintName='" + constraint + "'";
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
                        x.toArray(new SqlExecuteCommand[0]))
                )
                .collect(Collectors.toList()));
    }

    private boolean requiresCreateStateTable() throws SQLException {
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
        return createStateTable;
    }

    @Override
    public void modifyIndexes(boolean enable) {
        mainToolBase.out.println("Index " + (enable ? "enable" : "disable") + " not supported on PostgreSQL!");
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    public int toSupportedSqlType(int sqlType) {
        switch (sqlType) {
            // postgresql does not support Unicode character types
            case Types.NCHAR:
                return Types.CHAR;
            case Types.NVARCHAR:
                return Types.VARCHAR;
            case Types.LONGNVARCHAR:
                return Types.LONGVARCHAR;
            // postgresql does not support BLOBs and CLOBs in the JDBC driver
            case Types.BLOB:
                return Types.VARBINARY;
            case Types.CLOB:
                return Types.LONGVARCHAR;
            case Types.NCLOB:
                return Types.LONGVARCHAR;
        }
        return sqlType;
    }

    @Override
    public Object toSupportedValue(int sqlType, ColumnInfo columnInfo, Object value) {
        // postgres doesn't support storing NULL (\0x00) characters in text fields
        if (value instanceof String) {
            return ((String) value).replace("\u0000", "");
        }
        return value;
    }

    @Override
    public boolean canCreateBlobs() {
        return false;
    }

    @Override
    public Object convertCharacterStreamInput(Reader reader) {
        if (reader instanceof ChunkedReader) {
            return ((ChunkedReader) reader).readAsString();
        } else {
            // let the implementation handle this
            return reader;
        }
    }

    private String currentSchema() throws SQLException {
        if (cachedSchema == null) {
            // get current schema and create state table
            String schema;
            try (Connection con = mainToolBase.getReadOnlyConnection()) {
                schema = con.getSchema(); // get current schema
            }
            this.cachedSchema = schema;
            return schema;
        } else {
            return cachedSchema;
        }
    }

    private static class IdentityInfo {
        String sequenceName;
        String columnName;

        IdentityInfo(String sequenceName, String columnName) {
            this.sequenceName = sequenceName;
            this.columnName = columnName;
        }
    }

    private static class ForeignKeyConstraint {
        final String name;
        final String tableName;
        final String targetTable;
        final List<SourceTargetColumn> columns;
        final String match_option;
        final String update_rule;
        final String delete_rule;
        final boolean is_deferrable;
        final boolean initially_deferred;

        public ForeignKeyConstraint(String name, String tableName, String targetTable, List<SourceTargetColumn> columns, String match_option, String update_rule, String delete_rule, boolean is_deferrable, boolean initially_deferred) {
            this.name = name;
            this.tableName = tableName;
            this.targetTable = targetTable;
            this.columns = columns;
            this.match_option = match_option;
            this.update_rule = update_rule;
            this.delete_rule = delete_rule;
            this.is_deferrable = is_deferrable;
            this.initially_deferred = initially_deferred;
        }
    }

    private static class SourceTargetColumn {
        final String source;
        final String target;

        public SourceTargetColumn(String source, String target) {
            this.source = source;
            this.target = target;
        }
    }
}
