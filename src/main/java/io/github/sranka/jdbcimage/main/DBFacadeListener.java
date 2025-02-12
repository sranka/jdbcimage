package io.github.sranka.jdbcimage.main;

import io.github.sranka.jdbcimage.ResultSetInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("SpellCheckingInspection")
public interface DBFacadeListener {
    static DBFacadeListener getInstance(String className){
        if (!className.contains(".")){
            className = "io.github.sranka.jdbcimage.main.listener."+className;
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
                .map(String::trim)
                .filter(x -> !x.isEmpty())
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
