package e2e;

import io.github.sranka.jdbcimage.main.Env;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class ToolSetupRule implements TestRule {
    private ByteArrayOutputStream outputStream;

    public String getOutput() {
        return outputStream == null ? "" : outputStream.toString();
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Env.setExitToolOnError(false);
                try {
                    outputStream = new ByteArrayOutputStream();
                    Env.out = new PrintStream(outputStream, true);
                    base.evaluate();
                } finally {
                    Env.out = System.out;
                    outputStream = null;
                    Env.setExitToolOnError(true);
                }
            }
        };
    }
}
