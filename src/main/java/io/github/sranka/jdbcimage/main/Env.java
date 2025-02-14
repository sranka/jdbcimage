package io.github.sranka.jdbcimage.main;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Env {
    private static final Log log = LogFactory.getFactory().getInstance(Env.class);
    private static boolean exitToolOnError = true;

    public static void setExitToolOnError(boolean exitToolOnError) {
        Env.exitToolOnError = exitToolOnError;
    }

    public static class ExitedException extends RuntimeException {
        public ExitedException(int code) {
            super("Exited with code " + code);
        }

        public ExitedException(int code, Throwable cause) {
            super("Exited with code " + code, cause);
        }
    }
    public static void exit(int code, Throwable err){
        log.debug("Exiting with code " + code, err);
        if (exitToolOnError){
            System.out.println("FAILED: " + err.getMessage());
            System.exit(code);
            return;
        }
        if (err == null){
            throw new ExitedException(code);
        }
        throw new ExitedException(code, err);
    }
}
