package io.github.sranka.jdbcimage.main;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.dbcp2.BasicDataSource;

import io.github.sranka.jdbcimage.LoggedUtils;
import io.github.sranka.jdbcimage.ResultSetInfo;

/**
 * Facade that isolates specifics of a particular database
 * with regard to operations used by import/export.
 *
 * @author zavora
 */
public abstract class DBFacade implements DBFacadeListener{
    public static String IGNORED_TABLES = System.getProperty("ignored_tables","");

    protected MainToolBase mainToolBase;
    protected List<Predicate<String>> ignoredTables;

    public List<DBFacadeListener> listeners = new ArrayList<>();

    @Override
    public void setToolBase(MainToolBase mainToolBase) {
        this.mainToolBase = mainToolBase;
    }

    public void addListeners(List<DBFacadeListener> listeners){
        for(DBFacadeListener l : listeners){
            l.setToolBase(mainToolBase);
            if (!this.listeners.contains(l)) this.listeners.add(l);
        }
    }

    /**
     * Checks whether the database table is ignored for import/export.
     * @return ignored?
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isTableIgnored(String tableName){
        if (tableName == null) return true;
        if (ignoredTables == null){
            ignoredTables = Stream.of(
                    IGNORED_TABLES.split(","))
                    .filter(x -> !x.trim().isEmpty())
                    .map(x -> {
                        Pattern pattern = Pattern.compile(x);
                        return (Predicate<String>) s -> pattern.matcher(s).matches();
                    })
                    .collect(Collectors.toList());

        }
        for (Predicate<String> check : ignoredTables) {
            if (check.test(tableName)){
                return true;
            }
        }
        return false;
    }

    /**
     * Setups data source.
     * @param bds datasource
     */
    public abstract void setupDataSource(BasicDataSource bds);

    /**
     * Gets a result set representing current user user tables and excludes all ignored tables.
     * @param con connection
     * @return result
     */
    public final List<String> getUserTables(Connection con) throws SQLException{
        List<String> dbUserTables = getDbUserTables(con);
        List<String> retVal = dbUserTables.stream().filter(x -> !isTableIgnored(x)).collect(Collectors.toList());
        if (retVal.size() != dbUserTables.size()){
            ArrayList<String> ignored = new ArrayList<>(dbUserTables);
            ignored.removeAll(retVal);
            LoggedUtils.info("Ignored tables: "+ ignored);
        }
        return retVal;
    }

    /**
     * See {@link #getUserTables(Connection)} , but includes all tables
     */
    protected abstract List<String> getDbUserTables(Connection con) throws SQLException;

    /**
     * Turns on/off table constraints.
     * @param enable true to enable
     */
    public abstract void modifyConstraints(boolean enable) throws SQLException;

    /**
     * Turns on/off table indexes.
     * @param enable true to enable
     */
    public abstract void modifyIndexes(boolean enable) throws SQLException;

    /**
     * Gets the SQL DML that truncates the content of a table.
     * @param tableName table
     * @return command to execute
     */
    public String getTruncateTableSql(String tableName){
        return "TRUNCATE TABLE "+escapeTableName(tableName);
    }

    /**
     * Escapes column name
     * @param s s
     * @return escaped column name so that it can be used in queries.
     */
    public String escapeColumnName(String s){
        return s;
    }
    /**
     * Escapes table name
     * @param s s
     * @return escaped table name so that it can be used in queries.
     */
    public String escapeTableName(String s){
        return s;
    }

    /**
     * Gets table information.
     * @return never null, but possibly empty table info
     */
    public TableInfo getTableInfo(String tableName){
        return new TableInfo(tableName);
    }

    /**
     * Converts the requested type to a DB-supported alternative.
     * @param sqlType SQL type defined in {@link java.sql.Types java.sql.Types}
     * @return supported type
     */
    public int toSupportedSqlType(int sqlType) {
        return sqlType;
    }
    /**
     * Converts the requested value to a DB-supported value.
     * @param value value to process
     * @return supported value
     */
    public Object toSupportedValue(int sqlType, Object value) {
        return value;
    }
    /**
     * Checks whether the database instance can create and use BLOB, CLOB and NCLOB instances.
     *
     * @return can create blobs?
     */
    public boolean canCreateBlobs(){
        return true;
    }
    /**
     * Postgresql does not support statement.setCharacterStream(Reader),
     * this method can be used to convert a supplied reader to String.
     *
     * @param reader reader to convert, never null
     * @return converted reader, identity by default
     */
    public Object convertCharacterStreamInput(Reader reader){
        return reader;
    }

    @SuppressWarnings("WeakerAccess")
    public static class TableInfo{
        private final String tableName;
        private Map<String, Object> data;
        private Map<String, String> tableColumns;

        public TableInfo(String tableName) {
            this.tableName = tableName;
        }

        @SuppressWarnings("unused")
        public String getTableName() {
            return tableName;
        }

        public Map<String, Object> getData(){
            if (data == null){
                data = new HashMap<>();
            }
            return data;
        }
        public void put(String key, Object value){
            getData().put(key, value);
        }
        public Object get(String key){
            return data == null?null:data.get(key);
        }

        public Map<String, String> getTableColumns() {
            return tableColumns;
        }

        public void setTableColumns(Map<String, String> tableColumns) {
            this.tableColumns = tableColumns;
        }
    }

    public void importStarted(){
        listeners.forEach(DBFacadeListener::importStarted);
    }
    public void importFinished(){
        listeners.forEach(DBFacadeListener::importFinished);
    }
    public void beforeImportTable(Connection con, String table, TableInfo tableInfo) throws SQLException{
        for (DBFacadeListener l: listeners) {
            l.beforeImportTable(con, table, tableInfo);
        }
    }
    public void beforeImportTableData(Connection con, String table, TableInfo tableInfo, ResultSetInfo fileInfo) throws SQLException{
        for (DBFacadeListener l: listeners) {
            l.beforeImportTableData(con, table, tableInfo, fileInfo);
        }
    }
    public void afterImportTable(Connection con, String table, TableInfo tableInfo) throws SQLException{
        for (DBFacadeListener l: listeners) {
            l.afterImportTable(con, table, tableInfo);
        }
    }
}
