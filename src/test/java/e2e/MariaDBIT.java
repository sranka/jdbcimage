package e2e;

import io.github.sranka.jdbcimage.RowData;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.MariaDBContainer;

import java.io.File;

import static e2e.ToolSetupRule.CONTAINER_LOG;
import static org.junit.Assert.assertArrayEquals;

public class MariaDBIT {
    @Rule
    public ToolSetupRule toolSetup = new ToolSetupRule();
    @SuppressWarnings({"resource"})
    @ClassRule
    public static MariaDBContainer<?> container = new MariaDBContainer<>("mariadb:11.7.2")
            .withLogConsumer(CONTAINER_LOG);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File createFile(String name) {
        return new File(temporaryFolder.getRoot(), name);
//        return new File("/tmp", name);
    }

    @Test
    public void testExportImportExport() throws Exception {
        toolSetup.execSqlFromResource(container, "/e2e/mariadb/example_table_drop.sql");
        toolSetup.execSqlFromResource(container, "/e2e/mariadb/example_table_create.sql");
        toolSetup.execSqlFromResource(container, "/e2e/mariadb/example_table_insert.sql");

        // export1
        System.out.println("----- EXPORT1 -----");
        File exportedFile1 = createFile("mariadb_export1.zip");
        toolSetup.execTool(container, "export", exportedFile1.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // import
        System.out.println("----- IMPORT -----");
        toolSetup.execSqlFromResource(container, "/e2e/mariadb/example_table_drop.sql");
        toolSetup.execSqlFromResource(container, "/e2e/mariadb/example_table_create.sql");
        toolSetup.execTool(container, "import", exportedFile1.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // export2
        System.out.println("----- EXPORT2 -----");
        File exportedFile2 = createFile("mariadb_export2.zip");
        toolSetup.execTool(container, "export", exportedFile2.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // finally compare content of exported files, it must be the same
        byte[] exampleTableKryo1 = TestUtils.getKryoDataFromZipFile(exportedFile1, "example_table");
        byte[] exampleTableKryo2 = TestUtils.getKryoDataFromZipFile(exportedFile2, "example_table");
        assertArrayEquals(exampleTableKryo1, exampleTableKryo2);

        // compare exportedBytes with stored data
        // dump to be able the differences
        toolSetup.execTool(container, "dump", exportedFile1.getPath()+"#example_table");
        System.out.println("----- DUMP -----");
        System.out.println(toolSetup.getOutput());

        byte[] expectedKryoBytes = TestUtils.getKryoDataFromZipResource("/e2e/mariadb/example_table.zip", "example_table");
        assertArrayEquals(expectedKryoBytes, exampleTableKryo1);
    }
    @Test
    public void testImportFromPostgres() throws Exception {
        toolSetup.execSqlFromResource(container, "/e2e/mariadb/example_table_drop.sql");
        toolSetup.execSqlFromResource(container, "/e2e/mariadb/example_table_create.sql");

        // import
        System.out.println("----- IMPORT -----");
        File otherdbFile = createFile("postgres_example_table.zip");
        TestUtils.copyResourceToFile("/e2e/postgres/example_table.zip", otherdbFile);
        toolSetup.execTool(container, "import", otherdbFile.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // export
        System.out.println("----- EXPORT -----");
        File exportedFile = createFile("mariadb_export3.zip");
        toolSetup.execTool(container, "export", exportedFile.getPath());
        System.out.println("-------------------");
        System.out.println(toolSetup.getOutput());

        // compare exportedBytes with stored data
        // dump to be able the differences
        toolSetup.execTool(container, "dump", exportedFile.getPath()+"#example_table");
        System.out.println("----- DUMP -----");
        System.out.println(toolSetup.getOutput());

        // compare columns, but exclude the timestamp column (updated at), it cannot be the same because mariadb timestamp is not zoned
        byte[] exportedTableKryo = TestUtils.getKryoDataFromZipFile(exportedFile, "example_table");
        RowData row = TestUtils.readFirstRowFromKryoData(exportedTableKryo);
        new ExampleTableData().ignoreUpdatedAtColumn().assertEquals(row);
    }

}
