package e2e;

import io.github.sranka.jdbcimage.main.Env;
import io.github.sranka.jdbcimage.main.JdbcImageMain;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcImageMainIT {
    @Rule
    public ToolSetupRule toolSetupRule = new ToolSetupRule();

    @Test
    public void printsHelp() throws Exception{
        JdbcImageMain.main("help");
        assertTrue(toolSetupRule.getOutput().contains("See documentation"));
    }
    @Test
    public void exitsOnUnknownAction() throws Exception{
        try {
            JdbcImageMain.main();
        } catch (Env.ExitedException ex){
            assertEquals(1, ex.getCode());
        }
    }
}
