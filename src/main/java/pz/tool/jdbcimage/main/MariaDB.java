package pz.tool.jdbcimage.main;

import org.apache.commons.dbcp2.BasicDataSource;
import pz.tool.jdbcimage.db.SqlExecuteCommand;
import pz.tool.jdbcimage.db.TableGroupedCommands;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DB facade for MariaDB.
 */
public class MariaDB extends DBFacade {
    private MainToolBase mainToolBase;

    public MariaDB(MainToolBase mainToolBase) {
        this.mainToolBase = mainToolBase;
    }

    @Override
    public void setupDataSource(BasicDataSource bds) {
        bds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        bds.setConnectionInitSqls(Arrays.asList("SET FOREIGN_KEY_CHECKS = 0"));
    }

    @Override
    public List<String> getUserTables(Connection con) throws SQLException {
        List<String> retVal = new ArrayList<>();
        try(ResultSet tables = con.getMetaData().getTables(con.getCatalog(), con.getSchema(), "%", new String[]{"TABLE"})){
            while(tables.next()) {
                String tableName = tables.getString(3);
                retVal.add(tableName);
            }
        }
        return retVal;
    }

    @Override
    public String escapeColumnName(String s) {
        return "`"+s+"`";
    }

    @Override
    public String escapeTableName(String s) {
        return "`"+s+"`";
    }

    @Override
    public void modifyConstraints(boolean enable) throws SQLException {
        mainToolBase.out.println("Foreign key checks disabled in created database connections.");
    }

    @Override
    public void modifyIndexes(boolean enable) throws SQLException {
        mainToolBase.out.println("Index " + (enable ? "enable" : "disable") + " not supported on PostgreSQL!");
    }

    @Override
    public boolean canCreateBlobs() {
        return false;
    }
}
