package pz.tool.jdbcimage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains logging utilities.
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
	 * Logs info message
	 * @param message info
	 */
	public static void info(String message){
		log.info(message);
	}
	/**
	 * Ignores the exception with message supplied.
	 * @param ignoreMessage message to display
	 * @param e exception to log
	 */
	public static void ignore(String ignoreMessage, Exception e){
		log.warn(ignoreMessage,e);
	}
}
