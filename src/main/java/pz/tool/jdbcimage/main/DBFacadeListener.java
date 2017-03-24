package pz.tool.jdbcimage.main;

import java.sql.Connection;
import java.sql.SQLException;

public interface DBFacadeListener {
    void setToolBase(MainToolBase mainToolBase);

    void importStarted();
    void importFinished();
    void afterImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException;
    void beforeImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException;
}
