package pz.tool.jdbcimage.main;

import pz.tool.jdbcimage.ResultSetInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface DBFacadeListener {
    static DBFacadeListener getInstance(String className){
        if (!className.contains(".")){
            className = "pz.tool.jdbcimage.main.listener."+className;
        }
        try {
            return (DBFacadeListener)Class.forName(className+"Listener").newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static List<DBFacadeListener> getInstances(String classNames){
        if (classNames == null || classNames.isEmpty()) {
            return Collections.emptyList();
        }

        return Stream.of(classNames.split(","))
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(x -> x.length()>0)
                .map(DBFacadeListener::getInstance)
                .collect(Collectors.toList());
    }


    void setToolBase(MainToolBase mainToolBase);

    void importStarted();
    void importFinished();
    void beforeImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException;
    void afterImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException;
    default void beforeImportTableData(Connection con, String table, DBFacade.TableInfo tableInfo, ResultSetInfo fileInfo) throws SQLException {
    }
}
