package e2e;

import org.apache.commons.compress.utils.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Objects;
import java.util.zip.DeflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipUtils {
    public static byte[] getKryoDataFromZipFile(File zip, String tableName) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(Files.newInputStream(zip.toPath()), bos);
        System.out.println("Kryo zip file: "+ Base64.getEncoder().encodeToString(bos.toByteArray()));
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
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            IOUtils.copy(Objects.requireNonNull(ZipUtils.class.getResourceAsStream(zipResourceName)), bos);
            System.out.println("Resource file: "+ Base64.getEncoder().encodeToString(bos.toByteArray()));
            bos.reset();
            IOUtils.copy(Files.newInputStream(tempFile.toPath()), bos);
            System.out.println("Copied file: "+ Base64.getEncoder().encodeToString(bos.toByteArray()));
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
