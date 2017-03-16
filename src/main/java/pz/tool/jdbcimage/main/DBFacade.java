package pz.tool.jdbcimage.main;

import org.apache.commons.dbcp2.BasicDataSource;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Facade that isolates specifics of a particular database
 * in regards to operations used by import/export.
 *
 * @author zavora
 */
public abstract class DBFacade {
    /**
     * Setups data source.
     * @param bds datasource
     */
    public abstract void setupDataSource(BasicDataSource bds);
    /**
     * Gets a result set representing current user user tables.
     * @param con connection
     * @return result
     */
    public abstract List<String> getUserTables(Connection con) throws SQLException;

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
     * @param identityColumn identity column or null
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
