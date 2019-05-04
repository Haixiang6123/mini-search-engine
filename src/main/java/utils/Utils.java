package utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

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

    public static int countFiles(Path path) {
        int fileCount = 0;
        try {
            File folder = path.toFile();
            for (File fileList : Objects.requireNonNull(folder.listFiles())) {
                if (fileList.isFile()) {
                    fileCount++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return fileCount;
    }


    public static <T> List<T> intersectLists(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();

        for (T t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }


    public static <T> List<T> unionLists(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<T>();

        set.addAll(list1);
        set.addAll(list2);

        return new ArrayList<T>(set);
    }

    public static void renameSegment(Path basePath, int segmentIndex, String originName, String newName) {
        File file = basePath.resolve("segment" + segmentIndex + "_" + originName).toFile();
        File tempFile = basePath.resolve("segment" + segmentIndex + "_" + newName).toFile();

        file.renameTo(tempFile);
    }
}
