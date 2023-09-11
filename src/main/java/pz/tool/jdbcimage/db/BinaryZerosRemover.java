package pz.tool.jdbcimage.db;

public class BinaryZerosRemover {
    public String removeBinaryZeros(String input) {
        if (input == null) return null;

        return input.replace("\u0000", "");
    }
}
