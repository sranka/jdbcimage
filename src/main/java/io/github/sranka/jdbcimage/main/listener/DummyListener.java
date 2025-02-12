package io.github.sranka.jdbcimage.main.listener;

import io.github.sranka.jdbcimage.main.DBFacade;
import io.github.sranka.jdbcimage.main.DBFacadeListener;
import io.github.sranka.jdbcimage.main.MainToolBase;

import java.sql.Connection;

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
    public void beforeImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) {
        System.out.println("DummyListener.beforeImportTable "+ table);
    }

    @Override
    public void afterImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) {
        System.out.println("DummyListener.afterImportTable" + table);
    }

    public String toString(){
        return "Dummy";
    }

}
