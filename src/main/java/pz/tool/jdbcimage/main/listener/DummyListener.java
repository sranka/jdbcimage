package pz.tool.jdbcimage.main.listener;

import pz.tool.jdbcimage.main.DBFacade;
import pz.tool.jdbcimage.main.DBFacadeListener;
import pz.tool.jdbcimage.main.MainToolBase;

import java.sql.Connection;
import java.sql.SQLException;

@SuppressWarnings("unused") // used via reflection
public class DummyListener implements DBFacadeListener {
    @Override
    public void setToolBase(MainToolBase mainToolBase) {
        System.out.println("DummyListener.setToolBase : " + mainToolBase);
    }


    @Override
    public void importStarted() {
        System.out.println("DummyListener.importStarted");
    }

    @Override
    public void importFinished() {
        System.out.println("DummyListener.importFinished");
    }

    @Override
    public void beforeImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException {
        System.out.println("DummyListener.beforeImportTable "+ table);
    }

    @Override
    public void afterImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException {
        System.out.println("DummyListener.afterImportTable" + table);
    }

    public String toString(){
        return "Dummy";
    }

}
