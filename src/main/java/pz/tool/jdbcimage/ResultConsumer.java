package pz.tool.jdbcimage;

import java.util.function.Consumer;

/**
 * Called in order to process results using generic instances provided. The consumer 
 * <code>accept</code> method is called to process every row in the result set.    
 * @author zavora
 */
public interface ResultConsumer<T> extends Consumer<T>{
	/**
	 * Called to process a single row of the result.
	 * @param t object to get row data from
	 */
	@Override
	public void accept(T t);

	/**
	 * Called before the result set rows are processed.
	 * @param rs result set
	 */
	public default void onStart(ResultSetInfo info){
	}
	/**
	 * Called upon finish of the processing.
	 */
	public default void onFinish(){
	}
	/**
	 * Called upon processing failure.
	 * @param Exception exception
	 */
	public default void onFailure(Exception e){
	}
}
