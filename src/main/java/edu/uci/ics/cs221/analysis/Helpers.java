package edu.uci.ics.cs221.analysis;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Helpers {
    public static void processFile(String filename, String subfile) {
        try {
            URL dictResource = Helpers.class.getClassLoader().getResource(filename);
            List<String> lines = Files.readAllLines(Paths.get(dictResource.toURI()));

            //write it back
            for (int i = 0; i < lines.size() - 1; i++) {
                String s = lines.get(i);
                String[] tokens = s.split("\\s+");
                if (tokens.length == 3) {
                    s = tokens[2] + ' ' + tokens[1];
                    lines.set(i, s);
                }
            }

            String resUrl = dictResource.toString();
            String myUrl = resUrl.substring(6, resUrl.lastIndexOf('/')) + '/' + subfile;

            Path myPath = Files.createFile(Paths.get(myUrl));
            Files.write(myPath, lines);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
