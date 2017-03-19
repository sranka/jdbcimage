package pz.tool.jdbcimage.main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.dbcp2.BasicDataSource;

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
        bds.setConnectionInitSqls(Collections.singletonList("SET FOREIGN_KEY_CHECKS = 0"));
    }

    @Override
    public List<String> getDbUserTables(Connection con) throws SQLException {
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
        if (!enable) mainToolBase.out.println("Foreign key checks disabled in created database connections.");
    }

    @Override
    public void modifyIndexes(boolean enable) throws SQLException {
        mainToolBase.out.println("Index " + (enable ? "enable" : "disable") + " not supported on MariaDB!");
    }

    @Override
    public boolean canCreateBlobs() {
        return false;
    }
}
