package edu.uci.ics.cs221.analysis;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class JapaneseTokenizer extends WordBreakTokenizer{
    public JapaneseTokenizer(){
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_jp.txt");
            List<String> lines = Files.readAllLines(Paths.get(dictResource.toURI()));
            dict.clear();
            // Get dictionary
            initDict(lines);
            System.out.println(dict.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
