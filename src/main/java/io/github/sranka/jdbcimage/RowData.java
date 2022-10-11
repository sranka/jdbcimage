package io.github.sranka.jdbcimage;

/**
 * Represents a single row of a table.
 * This class is mutable, it is used as a token during processing.
 * @author zavora
 */
public class RowData {
	/** about columns, filled when started */
	public ResultSetInfo info;
	/** values can hold JDBC types + InputStream for blobs */
	public Object[] values;
	
	public RowData(ResultSetInfo info){
		this.info = info;
		values = new Object[info.columns.length];
	}
}
