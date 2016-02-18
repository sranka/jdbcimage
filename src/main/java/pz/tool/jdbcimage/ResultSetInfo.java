package pz.tool.jdbcimage;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Represents information about the result so it can be serialized and compared.
 */
public class ResultSetInfo {
	public String[] columns;
	public int[] types;
	/** current database connection, filled when started ... intended to create blobs */
	public transient Connection connection;

	/**
	 * Serialization initializer.
	 */
	public ResultSetInfo(){
	}

	/**
	 * Initializes itself from meta data.
	 * @param meta meta data
	 * @throws SQLException
	 */
	public ResultSetInfo(ResultSetMetaData meta) throws SQLException{
		int columnCount = meta.getColumnCount();
		columns = new String[columnCount];
		types = new int[columnCount];
		for(int i=0; i<columnCount; i++){
			columns[i] = meta.getColumnName(i+1);
		}
		for(int i=0; i<columnCount; i++){
			types[i] = meta.getColumnType(i+1);
		}
	}
	
	/**
	 * Gets column count.
	 * @return column count
	 */
	public int getColumnCount(){
		return columns.length;
	}

    @Override
    public String toString() {
        return "ResultSetInfo{" +
                "columns=" + Arrays.toString(columns) +
                ", types=" + Arrays.toString(types) +
                '}';
    }
}
