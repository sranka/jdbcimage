package e2e;

import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Objects;
import java.util.zip.DeflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipUtils {
    public static byte[] getKryoDataFromZipFile(File zip, String tableName) throws Exception {
        try(ZipFile zipFile = new ZipFile(zip)){
            ZipEntry entry = Objects.requireNonNull(zipFile.getEntry(tableName));
            return IOUtils.toByteArray(new DeflaterInputStream(zipFile.getInputStream(entry)));
        }
    }
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static byte[] getKryoDataFromZipResource(String zipResourceName, String tableName) throws Exception {
        File tempFile = File.createTempFile("zip-utils",".zip");
        try {
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                IOUtils.copy(Objects.requireNonNull(ZipUtils.class.getResourceAsStream(zipResourceName)), fos);
            }
            return getKryoDataFromZipFile(tempFile, tableName);
        } finally {
            tempFile.delete();
        }
/*
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
            return IOUtils.toByteArray(new DeflaterInputStream(zipInputStream));
        }
*/

    }

}
