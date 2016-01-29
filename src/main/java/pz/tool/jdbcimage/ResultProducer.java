package pz.tool.jdbcimage;

/**
 * Pull parser that is called in order to obtain row data.
 *  
 * @author zavora
 */
public interface ResultProducer{
	/**
	 * Called to initialize the provider.
	 * @return initialized row data
	 */
	public RowData start();
	
	/**
	 * Invoked in order to fill the data supplied. 
	 * @param row row to fill in
	 * @return false if no data can be filled 
	 */
	public boolean fillData(RowData row);

	/**
	 * Called to inform about finished processing.
	 * @return initialized row data
	 */
	public void close();
}
