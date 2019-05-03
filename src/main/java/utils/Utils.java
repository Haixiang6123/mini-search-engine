package utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Utils {
    public static String stringifyHashMap(Map map) {
        String result = "";
        for (Object name : map.keySet()) {
            String key = name.toString();
            String value = map.get(name).toString();
            result = result + key + " " + value + "\n";
        }

        return result;
    }

    public static String stringifyList(List list) {
        return Arrays.asList(list).toString();
    }
    public static String stringifyMatrix(Object[][] matrix) {
        StringBuilder sb = new StringBuilder();
        for (Object[] row : matrix) {
            for (Object cell : row) {
                sb.append(cell).append("\t");
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    public static String sliceStringFromBuffer(ByteBuffer byteBuffer, int start, int length) {
        int index = 0;
        byte[] tempBytes = new byte[length];
        for (int position = start; position < start + length; position++) {
            tempBytes[index++] = byteBuffer.get();
        }

        return bytesToString(tempBytes);
    }
}
