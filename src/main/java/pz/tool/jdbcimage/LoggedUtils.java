package pz.tool.jdbcimage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains utilities that performs a specific action and 
 * only logs a failure of that action.
 */
public class LoggedUtils {
	private static final Log log = LogFactory.getLog(LoggedUtils.class);
			
	/**
	 * Closes the closable supplied and logs exception when occurs.
	 * @param closeable to close
	 */
	public static void close(AutoCloseable closeable){
		try{
			closeable.close();
		} catch(Exception e){
			log.warn("Unable to close '"+closeable+"'!",e);
		}
	}

	/**
	 * Ignores the exception with message supplied.
	 * @param closeable to close
	 */
	public static void ignore(String ignoreMessage, Exception e){
		log.warn(ignoreMessage,e);
	}
}
