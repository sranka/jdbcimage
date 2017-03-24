package pz.tool.jdbcimage.main.listener;

import pz.tool.jdbcimage.LoggedUtils;
import pz.tool.jdbcimage.main.DBFacade;
import pz.tool.jdbcimage.main.DBFacadeListener;
import pz.tool.jdbcimage.main.MainToolBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Restarts global sequence out of maximum value of "id" column on all imported tables after data import.
 */
@SuppressWarnings({"unused", "WeakerAccess"}) // used by reflection
public class OracleRestartGlobalSequenceListener implements DBFacadeListener{
    protected MainToolBase mainToolBase;
    protected AtomicLong maxValue; // import of tables can run in parallel
    protected String sequenceName = System.getProperty("OracleRestartGlobalSequence.sequenceName");
    protected String onFinishSqls = System.getProperty("OracleRestartGlobalSequence.sql",
            "" +
                    "declare\n" +
                    "  seq_notexist exception;\n" +
                    "  pragma exception_init (seq_notexist , -2289);\n" +
                    "begin\n" +
                    "  execute immediate 'drop sequence $1';\n" +
                    "  exception when seq_notexist then null;\n" +
                    "end;" +
                    "\n/" +
                    "CREATE SEQUENCE $1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH $2 CACHE 20 NOORDER NOCYCLE");

    @Override
    public void setToolBase(MainToolBase mainToolBase) {
        this.mainToolBase = mainToolBase;
    }

    @Override
    public void importStarted() {
        maxValue = new AtomicLong(0L);
    }

    @Override
    public void importFinished() {
        if (onFinishSqls == null || onFinishSqls.isEmpty()) return;
        if (sequenceName == null || sequenceName.isEmpty()) return;
        // update sequence
        final String newValue = String.valueOf(maxValue.longValue()+1);
        try(Connection con = mainToolBase.getWriteConnection()){
            try (Statement stmt = con.createStatement()) {
                Stream.of(onFinishSqls.split("\n/"))
                        .map(String::trim)
                        .filter(x -> x.length()>0)
                        .forEach(x -> {
                            String toExecute = x.replace("$1", sequenceName).replace("$2",newValue);
                            LoggedUtils.info("OracleRestartGlobalSequenceListener.execute: "+toExecute);
                            try {
                                stmt.execute(toExecute);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beforeImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException {
        // nothing to do
    }

    @Override
    public void afterImportTable(Connection con, String table, DBFacade.TableInfo tableInfo) throws SQLException {
        Map<String, String> tableColumns = tableInfo.getTableColumns();
        String id = tableColumns == null? null:tableColumns.get("id");
        if (id!=null) {
            try (Statement stmt = con.createStatement()) {
                try(ResultSet rs = stmt.executeQuery("select max(" + id + ") from " + table)){
                    rs.next();
                    long l = rs.getLong(1);
                    if (!rs.wasNull()) {
                        maxValue.accumulateAndGet(l, (current,newVal) -> newVal>current?newVal:current);
                    }
                }
            }
        }
    }
}
