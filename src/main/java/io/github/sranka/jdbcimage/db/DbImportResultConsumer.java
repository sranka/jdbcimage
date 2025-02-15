package io.github.sranka.jdbcimage.db;

import io.github.sranka.jdbcimage.ChunkedInputStream;
import io.github.sranka.jdbcimage.ChunkedReader;
import io.github.sranka.jdbcimage.LoggedUtils;
import io.github.sranka.jdbcimage.ResultConsumer;
import io.github.sranka.jdbcimage.ResultSetInfo;
import io.github.sranka.jdbcimage.RowData;
import io.github.sranka.jdbcimage.main.DBFacade;
import io.github.sranka.jdbcimage.main.Mssql;
import io.github.sranka.jdbcimage.main.Oracle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Import pushed data into a database.
 */
public class DbImportResultConsumer implements ResultConsumer<RowData> {
    private static final Log log = LogFactory.getLog(DbImportResultConsumer.class);
    public static int BATCH_SIZE = Integer.parseInt(System.getProperty("batch.size", "100"));

    private final String tableName;
    private final Connection con;
    private final DBFacade db;
    private final Map<String, String> actualColumns;
    // state
    int batchPosition; // current batch position
    long processedRows = -1;
    private Consumer<ResultSetInfo> notifyOnStartFn = (r) -> {
    };
    // initialize in on start
    private PreparedStatement stmt = null;
    private ResultSetInfo info;
    // mapping of input positions to SQL statement positions
    private Integer[] placeholderPositions;

    /**
     * Creates database importer.
     *
     * @param tableName     table to insert to
     * @param connection    connection to write rows to
     * @param db            used to escape table and column names
     * @param actualColumns list of actual columns to know what columns to skip
     *                      with a key being lower case of the name, value is the actual name
     */
    public DbImportResultConsumer(String tableName, Connection connection, DBFacade db, Map<String, String> actualColumns) {
        this.tableName = tableName;
        this.con = connection;
        this.db = db;
        this.actualColumns = actualColumns;
    }

    public void setNotifyOnStartFn(Consumer<ResultSetInfo> consumer) {
        this.notifyOnStartFn = consumer;
    }

    @Override
    public void onStart(ResultSetInfo info) {
        this.notifyOnStartFn.accept(info);
        // set connection to info, so blobs can be serialized without extra resources
        if (this.db.canCreateBlobs()) {
            info.connection = con;
        }
        // initialize batch position
        batchPosition = 0;
        processedRows = 0;

        this.info = info;
        String[] columns = info.columns;
        this.placeholderPositions = new Integer[columns.length];

        // create SQL and placeholder positions
        StringBuilder insertSQL = new StringBuilder(200);
        insertSQL.append("INSERT INTO ").append(db.escapeTableName(tableName)).append(" (");
        int pos = 1;
        for (int i = 0; i < columns.length; i++) {
            String column = actualColumns.get(columns[i].toLowerCase());
            if (column != null) {
                if (pos != 1) insertSQL.append(',');
                insertSQL.append(db.escapeColumnName(column));
                placeholderPositions[i] = pos++;
            } else {
                placeholderPositions[i] = null;
            }
        }
        if (pos == 1) {
            log.debug("No columns available for import!");
            return; // no columns to write
        }
        insertSQL.append(") VALUES (");
        for (int i = 0; i < pos - 2; i++) insertSQL.append("?,");
        insertSQL.append("?)");

        // prepare statement
        try {
            stmt = con.prepareStatement(insertSQL.toString());
        } catch (SQLException e) {
            LoggedUtils.ignore("Unable to prepare statement!", e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public void accept(RowData t) {
        if (stmt == null) return; // nothing to do
        try {
            for (int i = 0; i < placeholderPositions.length; i++) {
                Integer pos = placeholderPositions[i];
                if (pos != null) { // data not ignored
                    Object value = t.values[i];
                    int type = db.toSupportedSqlType(info.types[i]);
                    if (value == null) {
                        stmt.setNull(pos, type);
                    } else {
                        value = db.toSupportedValue(type, value);
                        switch (type) {
                            case Types.BIGINT:
                                stmt.setLong(pos, (Long) value);
                                break;
                            case Types.BINARY:
                                stmt.setBytes(pos, (byte[]) value);
                                break;
                            case Types.BOOLEAN:
                            case Types.BIT:
                                stmt.setBoolean(pos, (Boolean) value);
                                break;
                            case Types.CHAR:
                            case Types.VARCHAR:
                            case Mssql.Types.DATETIMEOFFSET:
                                stmt.setString(pos, (String) value);
                                break;
                            case Types.NCHAR:
                            case Types.NVARCHAR:
                                stmt.setNString(pos, (String) value);
                                break;
                            case Types.DATE:
                                stmt.setDate(pos, (Date) value);
                                break;
                            case Types.TIME:
                                stmt.setTime(pos, (Time) value);
                                break;
                            case Types.TIMESTAMP:
                                stmt.setTimestamp(pos, (Timestamp) value);
                                break;
                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                stmt.setBigDecimal(pos, (BigDecimal) value);
                                break;
                            case Types.DOUBLE:
                            case Oracle.Types.BINARY_DOUBLE:
                                stmt.setDouble(pos, (Double) value);
                                break;
                            case Types.INTEGER:
                                stmt.setInt(pos, (Integer) value);
                                break;
                            case Types.TINYINT:
                            case Types.SMALLINT:
                                stmt.setShort(pos, (Short) value);
                                break;
                            case Types.REAL:
                            case Types.FLOAT:
                                stmt.setFloat(pos, (Float) value);
                                break;
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                            case Types.BLOB:
                                if (value instanceof InputStream) {
                                    if (value instanceof ChunkedInputStream) {
                                        stmt.setBinaryStream(pos, (InputStream) value, ((ChunkedInputStream) value).length());
                                    } else {
                                        stmt.setBinaryStream(pos, (InputStream) value);
                                    }
                                } else if (value instanceof Blob) {
                                    stmt.setBlob(pos, (Blob) value);
                                } else if (value instanceof byte[]) {
                                    stmt.setBytes(pos, (byte[]) value);
                                } else {
                                    throw new IllegalStateException("Unexpected value found for blob: " + value);
                                }
                                break;
                            case Types.LONGVARCHAR:
                            case Types.CLOB:
                            case Types.LONGNVARCHAR:
                            case Types.NCLOB:
                                if (value instanceof Reader) {
                                    value = db.convertCharacterStreamInput((Reader) value);
                                    if (value instanceof ChunkedReader) {
                                        stmt.setCharacterStream(pos, (Reader) value, ((ChunkedReader) value).length());
                                    } else if (value instanceof Reader) {
                                        stmt.setCharacterStream(pos, (Reader) value);
                                    } else if (value instanceof CharSequence) {
                                        stmt.setString(pos, value.toString());
                                    } else {
                                        throw new IllegalStateException("Unexpected value found for clob: " + value);
                                    }
                                } else if (value instanceof Clob) {
                                    if (type == Types.LONGNVARCHAR || type == Types.NCLOB) {
                                        stmt.setNClob(pos, (NClob) value);
                                    } else {
                                        stmt.setClob(pos, (Clob) value);
                                    }
                                } else {
                                    throw new IllegalStateException("Unexpected value found for clob: " + value);
                                }
                                break;
                            case Mssql.Types.SQL_VARIANT:
                                stmt.setObject(pos, value);
                                break;
                            case Types.OTHER:
                                if (value instanceof String) {
                                    // requires stringtype=unspecified in postgres connection string
                                    stmt.setString(pos, (String) value);
                                    break;
                                }
                                throw new IllegalStateException("Unable to set SQL type: " + type + " for value: " + value);
                            default:
                                throw new IllegalStateException("Unable to set SQL type: " + type + " for value: " + value);
                        }
                    }
                }
            }
            stmt.addBatch();
            batchPosition++;
            if (batchPosition >= BATCH_SIZE) {
                stmt.executeBatch();
                con.commit();
                batchPosition = 0;
            }
            processedRows++;
        } catch (SQLException e) {
            LoggedUtils.ignore("Unable to rollback!", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public long onFinish() {
        closeStatement();
        return processedRows;
    }

    @Override
    public void onFailure(Exception ex) {
        try {
            con.rollback();
        } catch (SQLException e) {
            LoggedUtils.ignore("Unable to rollback!", e);
        }
        closeStatement();
    }

    private void closeStatement() {
        try {
            if (stmt != null) {
                try {
                    if (batchPosition != 0) {
                        stmt.executeBatch();
                        con.commit();
                    }
                } finally {
                    stmt.close();
                    stmt = null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
