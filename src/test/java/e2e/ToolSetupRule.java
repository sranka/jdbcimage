package e2e;

import io.github.sranka.jdbcimage.main.Env;
import io.github.sranka.jdbcimage.main.JdbcImageMain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.output.OutputFrame;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class ToolSetupRule implements TestRule {
    private ByteArrayOutputStream outputStream;

    public static Consumer<OutputFrame> CONTAINER_LOG = (frame) ->
            System.out.println(frame.getType()+": "+ frame.getUtf8StringWithoutLineEnding());

    public String getOutput() {
        return outputStream == null ? "" : outputStream.toString();
    }

    public void clearOutput() {
        if (outputStream != null) {
            outputStream.reset();
        }
    }

    public String[] toolArgs(JdbcDatabaseContainer<?> container, String action, String... rest) {
        List<String> args = new ArrayList<>(Arrays.asList(action,
                "-url=" + container.getJdbcUrl(),
                "-user=" + container.getUsername(),
                "-password=" + container.getPassword()
        ));
        args.addAll(Arrays.asList(rest));
        return args.toArray(new String[0]);
    }

    public void execSqlFromResource(JdbcDatabaseContainer<?> container, String sqlResourceName) throws Exception {
        // get SQL in a way compatible with Java 8
        Path resPath = Paths.get(Objects.requireNonNull(getClass().getResource(sqlResourceName)).toURI());
        String sql = new String(Files.readAllBytes(resPath), StandardCharsets.UTF_8);

        JdbcImageMain.main(toolArgs(container, "exec", "-sql=" + sql));
        System.clearProperty("sql");
        clearOutput();
    }

    public void execTool(JdbcDatabaseContainer<?> container, String action, String... rest) throws Exception {
        JdbcImageMain.main(toolArgs(container, action, rest));
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
