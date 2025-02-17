package e2e;

import io.github.sranka.jdbcimage.RowData;
import io.github.sranka.jdbcimage.kryo.KryoResultProducer;
import org.apache.commons.compress.utils.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.Objects;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class TestUtils {
    public static byte[] getKryoDataFromZipFile(File zip, String tableName) throws Exception {
        try(ZipFile zipFile = new ZipFile(zip)){
            ZipEntry entry = Objects.requireNonNull(zipFile.getEntry(tableName));
            return IOUtils.toByteArray(new InflaterInputStream(zipFile.getInputStream(entry)));
        }
    }
    public static byte[] getKryoDataFromZipResource(String zipResourceName, String tableName) throws Exception {
        URL zipURL = Objects.requireNonNull(TestUtils.class.getResource(zipResourceName));

        try(InputStream inputStream = zipURL.openStream()){
            ZipInputStream zipInputStream = new ZipInputStream(inputStream);
            ZipEntry zipEntry;
            do {
                zipEntry = zipInputStream.getNextEntry();
                if(zipEntry == null) break;
            }
            while(!tableName.equals(zipEntry.getName()));
            if (zipEntry == null) {
                throw new IllegalStateException("Table " + tableName + " not found");
            }
            return IOUtils.toByteArray(new InflaterInputStream(zipInputStream));
        }
    }
    public static void copyResourceToFile(String resourceName, File file) throws Exception {
        IOUtils.copy(
                Objects.requireNonNull(TestUtils.class.getResourceAsStream(resourceName)),
                Files.newOutputStream(file.toPath())
        );
    }
    public static RowData readFirstRowFromKryoData(byte[] exportedTableKryo) {
        KryoResultProducer producer = new KryoResultProducer(new ByteArrayInputStream(exportedTableKryo));
        RowData row = producer.start();
        producer.fillData(row);
        producer.close();
        return row;
    }
}
