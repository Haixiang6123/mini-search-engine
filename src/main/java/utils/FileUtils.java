package utils;

import edu.uci.ics.cs221.storage.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FileUtils {
    public static String readFileAsString(File file, ReadFileCallback callback) {
        StringBuilder content = new StringBuilder();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // Build a string
                content.append(line).append("\n");
                // Read file callback
                if (callback != null) {
                    callback.callback(line);
                }
            }
            content = new StringBuilder(content.substring(0, content.length() - 1));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return content.toString();
    }
}
