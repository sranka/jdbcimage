package e2e;

import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class ZipUtils {
    public static byte[] getTableDataFromZip(File zip, String tableName) throws Exception {
        try(ZipFile zipFile = new ZipFile(zip)){
            ZipEntry entry = Objects.requireNonNull(zipFile.getEntry(tableName));
            return IOUtils.toByteArray(zipFile.getInputStream(entry));
        }
    }
    public static byte[] getTableDataFromZipResource(String zipResourceName, String tableName) throws Exception {
        URL zipURL = Objects.requireNonNull(ZipUtils.class.getResource(zipResourceName));

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
            return IOUtils.toByteArray(zipInputStream);
        }
    }

}
