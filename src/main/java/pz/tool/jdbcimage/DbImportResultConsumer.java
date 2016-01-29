package pz.tool.jdbcimage;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

///////////////////////////
// Database Importer
///////////////////////////
public class DbImportResultConsumer implements ResultConsumer<RowData>{
	public static int BATCH_SIZE = 20000;
	
	private String tableName;
	private Connection con;
	private Map<String,String> actualColumns; 
	
	// initialize in on start
	private PreparedStatement stmt = null;
	private ResultSetInfo info;
	// mapping of input positions to SQL statement positions 
	private Integer[] placeholderPositions;

	// state
	int batchPosition; // current batch position

	/**
	 * Creates database importer.
	 * @param tableName table to insert to
	 * @param connection connection to write rows to
	 * @param list of actual columns to know what columns to skip 
	 * with a key being lower case of the name, value is the actual name
	 */
	public DbImportResultConsumer(String tableName, Connection connection, Map<String,String> actualColumns) {
		this.tableName = tableName;
		this.con = connection;
		this.actualColumns = actualColumns;
	}

	@Override
	public void onStart(ResultSetInfo info) {
		// set connection to info, so blobs can be serialized without extra resources
		info.connection = con;
		// initialize batch position
		batchPosition = 0;
		
		this.info = info;
		String[] columns = info.columns;
		this.placeholderPositions = new Integer[columns.length];

		// create SQL and placeholder positions
		StringBuilder insertSQL = new StringBuilder(200);
		insertSQL.append("INSERT INTO ").append(tableName).append(" (");
		int pos = 1;
		for(int i=0; i<columns.length; i++){
			String column = actualColumns.get(columns[i].toLowerCase());
			if (column!=null){
				if (pos!=1) insertSQL.append(',');
				insertSQL.append(column);
				placeholderPositions[i] = pos++;
			} else{
				placeholderPositions[i] = null;
			}
		}
		if (pos == 1) {
			// TODO log this out to that the table import was skipped
			return; // no columns to write
		}
		insertSQL.append(") VALUES (");
		for(int i=0; i<pos-2; i++) insertSQL.append("?,");
		insertSQL.append("?)");
		
		// prepare statement
		try {
			stmt = con.prepareStatement(insertSQL.toString());
		} catch (SQLException e) {
			// TODO log
			throw new RuntimeException(e);
		}
	}

	@Override
	public void accept(RowData t) {
		if (stmt == null) return; // nothing to do
		try{
			for(int i=0; i<placeholderPositions.length; i++){
				Integer pos = placeholderPositions[i];
				if (pos != null){ // data not ignored
					Object value = t.values[i];
					int type = info.types[i];
					if (value == null){
						stmt.setNull(pos, type);
					} else if (value instanceof InputStream){
						if (value instanceof ChunkedInputStream){
							stmt.setBinaryStream(pos, (InputStream)value, ((ChunkedInputStream)value).length());
						} else{
							stmt.setBinaryStream(pos, (InputStream)value, ((ChunkedInputStream)value).length());
						}
					} else if (value instanceof Blob){
						stmt.setBlob(pos, (Blob)value);
					} else if (value instanceof byte[]){
						stmt.setBytes(pos, (byte[])value);
					} else{
						stmt.setObject(pos, value, type);
					}
				}
			}
			stmt.addBatch();batchPosition++;
			if (batchPosition>=BATCH_SIZE){
				stmt.executeBatch();
				batchPosition = 0;
			}
			stmt.executeBatch();
			con.commit();
		} catch (SQLException e) {
			// TODO log
			throw new RuntimeException(e);
		}
	}

	
	@Override
	public void onFinish() {
		closeStatement();
	}

	@Override
	public void onFailure(Exception ex) {
		try{
			con.rollback();
		} catch(SQLException e){
			// TODO log
		}
		closeStatement();
	}

	private void closeStatement(){
		try{
			if (stmt!=null){
				try{
					if (batchPosition!=0){
						stmt.executeBatch();
						con.commit();
					}
				} finally{
					stmt.close();
					stmt = null;
				}
			}
		} catch(SQLException e){
			throw new RuntimeException(e);
		}
	}
}
