package pz.tool.jdbcimage.main;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.dbcp2.BasicDataSource;

import pz.tool.jdbcimage.LoggedUtils;

/**
 * Facade that isolates specifics of a particular database
 * in regards to operations used by import/export.
 *
 * @author zavora
 */
public abstract class DBFacade {
    public static String IGNORED_TABLES = System.getProperty("ignored_tables","");

    protected List<String> ignoredTables;
    /**
     * Checks whether the database table is ignored for import/export.
     * @return ignored?
     */
    public boolean isTableIgnored(String tableName){
        if (tableName == null) return true;
        if (ignoredTables == null){
            ignoredTables = Stream.of(
                    IGNORED_TABLES.split(","))
                    .filter(x -> x!=null && x.trim().length()>0)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());

        }
        return ignoredTables.contains(tableName.toLowerCase());
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
     * Called before rows are inserted into table.
     * @param con connection
     * @param table table name
     * @param identityInfo identity column or null
     */
    public void afterImportTable(Connection con, String table, Object identityInfo) throws SQLException{
    }

    /**
     * Called before rows are inserted into table.
     * @param con connection
     * @param table table name
     * @param identityInfo identity information about the table
     */
    public void beforeImportTable(Connection con, String table, Object identityInfo) throws SQLException{
    }
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
     * Returns tables with identity column.
     * @return table to identity column name
     */
    public Map<String, ?> getTablesWithIdentityColumn() {
        return Collections.emptyMap();
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
}
