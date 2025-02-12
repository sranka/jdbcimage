package io.github.sranka.jdbcimage.main;

import io.github.sranka.jdbcimage.LoggedUtils;
import io.github.sranka.jdbcimage.ResultProducerRunner;
import io.github.sranka.jdbcimage.db.DbImportResultConsumer;
import io.github.sranka.jdbcimage.kryo.KryoResultProducer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Import a single table from the database.
 */
public class SingleTableImport extends MainToolBase {

    public static void main(String... args) throws Exception {
        args = setupSystemProperties(args);

        try (SingleTableImport tool = new SingleTableImport()) {
            if (tool.tool_table == null || tool.tool_table.isEmpty()) {
                if (args.length == 0 || args[0].isEmpty()) {
                    throw new IllegalArgumentException("Expected table file as an argument, but no or empty argument supplied!");
                } else {
                    tool.tool_table = args[0];
                }
            }

            tool.run();
        }
    }

    public void run() throws SQLException, IOException {
        File file = new File(tool_table);
        if (tool_table.contains("/")) {
            tool_table = tool_table.substring(tool_table.lastIndexOf("/") + 1);
        }

        out.println("Importing from: " + file);
        out.println("Reset time: " + truncateTable(tool_table));
        long time = System.currentTimeMillis();
        dbFacade.importStarted();
        out.println("Imported rows: " + importTable(tool_table, file, dbFacade.getTableInfo(tool_table)));
        dbFacade.importFinished();
        out.println("Import time: " + Duration.ofMillis(System.currentTimeMillis() - time));
    }

    public Duration truncateTable(String tableName) throws SQLException {
        long start = System.currentTimeMillis();
        Connection con = getWriteConnection();
        boolean committed = false;
        try {
            // delete table
            try (Statement del = con.createStatement()) {
                del.executeUpdate(dbFacade.getTruncateTableSql(tableName));
            }
            con.commit();
            committed = true;
            return Duration.ofMillis(System.currentTimeMillis() - start);
        } finally {
            if (!committed) {
                try {
                    con.rollback();
                } catch (Exception e) {
                    LoggedUtils.ignore("Unable to rollback!", e);
                }
            }
            LoggedUtils.close(con);
        }
    }

    /**
     * Imports specific table.
     *
     * @param tableName tables nam
     * @param file      usually the same table name, might differ in lower/upper case
     * @param tableInfo table information
     * @return time spent
     */
    public long importTable(String tableName, File file, DBFacade.TableInfo tableInfo) throws SQLException, IOException {
        InputStream in = toResultInput(file);
        KryoResultProducer producer = new KryoResultProducer(in);

        Connection con = getWriteConnection();

        try {
            // detect actual columns, ignore case
            Map<String, String> actualColumns = new HashMap<>();
            try (Statement detect = con.createStatement()) {
                try (ResultSet rs = detect.executeQuery("SELECT * FROM " + tableName + " WHERE 0=1")) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int cols = metaData.getColumnCount();
                    for (int i = 1; i <= cols; i++) {
                        String colName = metaData.getColumnName(i);
                        actualColumns.put(colName.toLowerCase(), colName);
                    }
                }
            } finally {
                con.rollback(); // no changes
            }
            // import data
            tableInfo.setTableColumns(actualColumns);
            dbFacade.beforeImportTable(con, tableName, tableInfo);
            DbImportResultConsumer consumer = new DbImportResultConsumer(tableName, con, dbFacade, actualColumns);
            consumer.setNotifyOnStartFn(fileInfo -> {
                try {
                    dbFacade.beforeImportTableData(con, tableName, tableInfo, fileInfo);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            ResultProducerRunner runner = new ResultProducerRunner(producer, consumer);
            long rows = runner.run();
            dbFacade.afterImportTable(con, tableName, tableInfo);

            return rows;
        } finally {
            LoggedUtils.close(con);
            LoggedUtils.close(in);
        }
    }

}
