package utils;

import edu.uci.ics.cs221.storage.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FileUtils {
    public static String readFileAsString(File file, ReadFileCallback callback) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // Build a string
                stringBuilder.append(line);
                // Read file callback
                if (callback != null) {
                    callback.callback(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return stringBuilder.toString();
    }
}
