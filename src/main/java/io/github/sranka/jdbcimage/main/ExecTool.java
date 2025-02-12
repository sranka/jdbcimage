package io.github.sranka.jdbcimage.main;

import io.github.sranka.jdbcimage.LoggedUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

/**
 * Executes the script supplied on input
 */
public class ExecTool extends MainToolBase {
    public String sql = System.getProperty("sql", null);

    public static void main(String... args) throws Exception {
        //noinspection UnusedAssignment
        args = setupSystemProperties(args);

        try (ExecTool tool = new ExecTool()) {
            tool.run();
        } catch (IllegalArgumentException e) {
            System.out.println("FAILED: " + e.getMessage());
            System.exit(1);
        }
    }

    public void run() throws Exception {
        if (sql == null) {
            out.println("Reading SQL commands from standard input.");
            java.util.Scanner s = new java.util.Scanner(System.in).useDelimiter("\\A");
            sql = s.hasNext() ? s.next() : "";
        }

        Connection con = null;
        try {
            con = getWriteConnection();
            try (Statement stmt = con.createStatement()) {
                Stream.of(sql.split("\n/"))
                        .map(String::trim)
                        .filter(x -> !x.isEmpty())
                        .forEach(x -> {
                            out.println("================================");
                            out.println(x);
                            out.println("--------------------------------");
                            try {
                                if (stmt.execute(x)) {
                                    int rowCount = 0;
                                    try (ResultSet rs = stmt.getResultSet()) {
                                        ResultSetMetaData metaData = rs.getMetaData();
                                        int colCount = metaData.getColumnCount();
                                        String[] columns = new String[colCount + 1];
                                        for (int i = 1; i <= colCount; i++) {
                                            columns[i] = metaData.getColumnName(i) + " [" + metaData.getColumnTypeName(i) + "]";
                                        }
                                        while (rs.next()) {
                                            rowCount++;
                                            for (int i = 1; i <= colCount; i++) {
                                                Object object = rs.getObject(i);
                                                out.print(columns[i]);
                                                out.print(" : ");
                                                out.println(rs.wasNull() ? "null" : String.valueOf(object));
                                            }
                                            out.println("--------------------------------");
                                        }
                                    }
                                    out.println("Row count: " + rowCount);
                                } else {
                                    out.println("Update count: " + stmt.getUpdateCount());
                                }
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            out.println("================================");
                        });
            }
            con.commit();
            con.close();
            con = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (con != null) try {
                con.rollback();
            } catch (Exception e2) {
                LoggedUtils.ignore("Cannot rollback", e2);
            }
            if (con != null) try {
                con.close();
            } catch (Exception e2) {
                LoggedUtils.ignore("Cannot close", e2);
            }
        }
    }
}
