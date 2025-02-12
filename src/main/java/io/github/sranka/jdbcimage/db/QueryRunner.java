package io.github.sranka.jdbcimage.db;

import io.github.sranka.jdbcimage.LoggedUtils;
import io.github.sranka.jdbcimage.ResultConsumer;
import io.github.sranka.jdbcimage.ResultSetInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Runs a specified query and pushes each row to a specified consumer.
 */
public class QueryRunner implements Runnable {
    public static int FETCH_SIZE = 100;

    private final Connection con;
    private final String query;
    private final ResultConsumer<ResultSet> consumer;
    // rows processed
    private long rows = 0;

    public QueryRunner(Connection con, String query, ResultConsumer<ResultSet> consumer) {
        this.con = con;
        this.query = query;
        this.consumer = consumer;
    }

    public void run() {
        try (Statement stmt = createStatement()) {
            stmt.setFetchSize(FETCH_SIZE);
            try (ResultSet rs = executeQuery(stmt)) {
                consumer.onStart(new ResultSetInfo(rs.getMetaData()));
                while (rs.next()) {
                    rows++;
                    consumer.accept(rs);
                }
                consumer.onFinish();
            }
        } catch (Exception e) {
            consumer.onFailure(e);
            throw new RuntimeException(e);
        } finally {
            try {
                con.rollback(); // nothing to commit
            } catch (SQLException e) {
                LoggedUtils.ignore("Unable to rollback!", e);
            }
        }
    }

    protected Statement createStatement() throws SQLException {
        return con.createStatement();
    }

    protected ResultSet executeQuery(Statement stmt) throws SQLException {
        return stmt.executeQuery(query);
    }

    public long getProcessedRows() {
        return rows;
    }
}
