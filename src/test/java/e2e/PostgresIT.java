package e2e;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.File;

import static e2e.ToolSetupRule.CONTAINER_LOG;
import static org.junit.Assert.assertArrayEquals;

public class PostgresIT {
    @Rule
    public ToolSetupRule toolSetup = new ToolSetupRule();
    @SuppressWarnings({"SpellCheckingInspection", "resource"})
    @Rule
    public PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:13-alpine").withUrlParam("stringtype", "unspecified").withLogConsumer(CONTAINER_LOG);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File createFile(String name) {
        // use this for debugging problems:
        // return new File("/tmp", name);
        return new File(temporaryFolder.getRoot(), name);
    }

    @Test
    public void testExportImportExport() throws Exception {
        toolSetup.execSqlFromResource(container, "/e2e/postgres/example_table_drop.sql");
        toolSetup.execSqlFromResource(container, "/e2e/postgres/example_table_create.sql");
        toolSetup.execSqlFromResource(container, "/e2e/postgres/example_table_insert.sql");

        // export1
        System.out.println("----- EXPORT1 -----");
        File exportedFile1 = createFile("pg_export1.zip");
        toolSetup.execTool(container, "export", exportedFile1.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // import
        System.out.println("----- IMPORT -----");
        toolSetup.execSqlFromResource(container, "/e2e/postgres/example_table_drop.sql");
        toolSetup.execSqlFromResource(container, "/e2e/postgres/example_table_create.sql");
        toolSetup.execTool(container, "import", exportedFile1.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // export2
        System.out.println("----- EXPORT2 -----");
        File exportedFile2 = createFile("pg_export2.zip");
        toolSetup.execTool(container, "export", exportedFile2.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // finally compare content of exported files, it must be the same
        byte[] exampleTableKryo1 = ZipUtils.getKryoDataFromZipFile(exportedFile1, "example_table");
        byte[] exampleTableKryo2 = ZipUtils.getKryoDataFromZipFile(exportedFile2, "example_table");
        assertArrayEquals(exampleTableKryo1, exampleTableKryo2);

        // compare exportedBytes with stored data
        // dump to be able the differences
        toolSetup.execTool(container, "dump", exportedFile1.getPath()+"#example_table");
        System.out.println("----- DUMP -----");
        System.out.println(toolSetup.getOutput());

        byte[] expectedKryoBytes = ZipUtils.getKryoDataFromZipResource("/e2e/postgres/example_table.zip", "example_table");
        assertArrayEquals(expectedKryoBytes, exampleTableKryo1);
    }
}
