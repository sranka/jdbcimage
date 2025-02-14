package io.github.sranka.jdbcimage.main;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.PrintStream;

public class Env {
    private static final Log log = LogFactory.getFactory().getInstance(Env.class);
    public static PrintStream out = System.out;
    private static boolean exitToolOnError = true;


    public static void setExitToolOnError(boolean exitToolOnError) {
        Env.exitToolOnError = exitToolOnError;
    }

    public static class ExitedException extends RuntimeException {
        private final int code;
        public ExitedException(int code) {
            super("Exited with code " + code);
            this.code = code;
        }

        public ExitedException(int code, Throwable cause) {
            super("Exited with code " + code, cause);
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }
    public static void exit(int code, Throwable err){
        log.debug("Exiting with code " + code, err);
        if (exitToolOnError){
            Env.out.println("FAILED: " + err.getMessage());
            System.exit(code);
            return;
        }
        if (err == null){
            throw new ExitedException(code);
        }
        throw new ExitedException(code, err);
    }
}
