package io.github.sranka.jdbcimage.main;

import io.github.sranka.jdbcimage.LoggedUtils;
import io.github.sranka.jdbcimage.db.QueryRunner;
import io.github.sranka.jdbcimage.kryo.KryoResultSetConsumer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;

/**
 * Performs export of a single table.
 */
public class SingleTableExport extends MainToolBase {

    public static void main(String... args) throws Exception {
        //noinspection UnusedAssignment
        args = setupSystemProperties(args);

        try (SingleTableExport tool = new SingleTableExport()) {
            tool.run();
        }
    }

    public void run() throws SQLException, IOException {
        File file = new File(tool_table);
        if (tool_table.contains("/")) {
            tool_table = tool_table.substring(tool_table.lastIndexOf("/") + 1);
        }

        long time = System.currentTimeMillis();
        long rows = exportTable(tool_table, file);
        out.println("Rows exported: " + rows);
        out.println("Processing time: " + Duration.ofMillis(System.currentTimeMillis() - time));
        out.println("Saved to: " + file);
    }

    public long exportTable(String tableName, File file) throws SQLException, IOException {
        OutputStream out = toResultOutput(file);
        KryoResultSetConsumer serializer = new KryoResultSetConsumer(out);
        boolean failed = true;

        Connection con = getReadOnlyConnection();
        QueryRunner runner = null;
        try {
            runner = new QueryRunner(con, getSelectStatement(tableName, con), serializer);
            runner.run();
            failed = false;
            return runner.getProcessedRows();
        } finally {
            LoggedUtils.close(con);
            // close the file
            LoggedUtils.close(out);
            // delete the output if it failed or zero rows read
            if (failed || ((runner.getProcessedRows() == 0) && isIgnoreEmptyTables())) {
                if (!file.delete()) {
                    LoggedUtils.ignore("Unable to delete " + file, null);
                }
            }
        }
    }

    public String getSelectStatement(String tableName, Connection con) throws SQLException {
        // get column names, VARBINARY and BLOBs must be last to avoid
        // ORA-24816: Expanded non-LONG bind data supplied
        StringBuilder columns = new StringBuilder();
        boolean hasId = false;
        try (Statement stmt = con.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + dbFacade.escapeTableName(tableName) + " WHERE 0=1")) {
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                boolean needComma = false;
                for (int i = 0; i < columnCount; i++) {
                    if ("id".equalsIgnoreCase(meta.getColumnName(i + 1))) {
                        hasId = true;
                    }
                    int colType = meta.getColumnType(i + 1);
                    if (colType != Types.BLOB && colType != Types.VARBINARY) {
                        if (needComma) columns.append(",");
                        else needComma = true;
                        columns.append(dbFacade.escapeColumnName(meta.getColumnName(i + 1)));
                    }
                }
                for (int i = 0; i < columnCount; i++) {
                    if (meta.getColumnType(i + 1) == Types.VARBINARY) {
                        if (needComma) columns.append(",");
                        else needComma = true;
                        columns.append(dbFacade.escapeColumnName(meta.getColumnName(i + 1)));
                    }
                }
                for (int i = 0; i < columnCount; i++) {
                    if (meta.getColumnType(i + 1) == Types.BLOB) {
                        if (needComma) columns.append(",");
                        else needComma = true;
                        columns.append(dbFacade.escapeColumnName(meta.getColumnName(i + 1)));
                    }
                }
            }
        }

        String retVal = "SELECT " + columns + " FROM " + dbFacade.escapeTableName(tableName);
        if (hasId) {
            retVal += " ORDER BY id";
        }
        return retVal;
    }
}
