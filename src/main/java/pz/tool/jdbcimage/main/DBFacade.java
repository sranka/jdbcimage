package pz.tool.jdbcimage.main;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
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
    public abstract ResultSet getUserTables(Connection con) throws SQLException;

    /**
     * Turns on/off table constraints.
     * @param enable true to enable
     * @return operation time
     * @throws SQLException
     */
    public abstract Duration modifyConstraints(boolean enable) throws SQLException;

    /**
     * Turns on/off table indexes.
     * @param enable true to enable
     * @return operation time
     * @throws SQLException
     */
    public abstract Duration modifyIndexes(boolean enable) throws SQLException;

    /**
     * Called before rows are inserted into table.
     * @param con connection
     * @param table table name
     * @param hasIdentityColumn indicates whether the table has identity column
     */
    public void afterImportTable(Connection con, String table, boolean hasIdentityColumn) throws SQLException{
    }

    /**
     * Called before rows are inserted into table.
     * @param con connection
     * @param table table name
     * @param hasIdentityColumn indicates whether the table has identity column
     */
    public void beforeImportTable(Connection con, String table, boolean hasIdentityColumn) throws SQLException{
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
     * Returns tables that have identity columns.
     * @return set of tables that contain identity columns
     */
    public Set<String> getTablesWithIdentityColumns() {
        return Collections.emptySet();
    }
}
