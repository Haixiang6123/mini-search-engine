package utils;

import edu.uci.ics.cs221.inverted.MergedWordBlock;
import edu.uci.ics.cs221.inverted.WordBlock;

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

    public static List<MergedWordBlock> mergeWordBlocks(List<WordBlock> list1, List<WordBlock> list2) {
        List<MergedWordBlock> newList = new ArrayList<>();
        for (WordBlock wordBlock1 : list1) {
            MergedWordBlock mergedWordBlock = new MergedWordBlock(true);
            mergedWordBlock.leftWordBlock = wordBlock1;

            for (WordBlock wordBlock2 : list2) {
                if (wordBlock1.word.equals(wordBlock2.word)) {
                    mergedWordBlock.isSingle = false;
                    mergedWordBlock.rightWordBlock = wordBlock2;
                    break;
                }
            }
            newList.add(mergedWordBlock);
        }

        for (WordBlock wordBlock2 : list2) {
            boolean found = false;
            for (WordBlock wordBlock1 : list1) {
                if (wordBlock2.word.equals(wordBlock1.word)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                MergedWordBlock mergedWordBlock = new MergedWordBlock(true);
                mergedWordBlock.rightWordBlock = wordBlock2;
                newList.add(mergedWordBlock);
            }
        }

        return newList;
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

    public static void renameStore(Path basePath, int segmentIndex, String newName) {
        File file = basePath.resolve("store" + segmentIndex).toFile();
        File tempFile = basePath.resolve("store" + segmentIndex + "_" + newName).toFile();

        file.renameTo(tempFile);
    }
}
