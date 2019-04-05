package edu.uci.ics.cs221.analysis;

import java.util.*;

/**
 * Project 1, task 1: Implement a simple tokenizer based on punctuations and white spaces.
 *
 * For example: the text "I am Happy Today!" should be tokenized to ["happy", "today"].
 *
 * Requirements:
 *  - White spaces (space, tab, newline, etc..) and punctuations provided below should be used to tokenize the text.
 *  - White spaces and punctuations should be removed from the result tokens.
 *  - All tokens should be converted to lower case.
 *  - Stop words should be filtered out. Use the stop word list provided in `StopWords.java`
 *
 */
public class PunctuationTokenizer implements Tokenizer {

    public static Set<String> punctuations = new HashSet<>();
    public static String punctuationsPattern = "[,.;?! ]";

    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public PunctuationTokenizer() {}

    public List<String> tokenize(String text) {
        List<String> tokenList = new ArrayList<>();
        // To lowercase
        text = text.toLowerCase();
        // Remove punctuations
        String[] rawTokens = text.split(PunctuationTokenizer.punctuationsPattern);
        // Filter stop words
        for (String rawToken : rawTokens) {
            if (!StopWords.stopWords.contains(rawToken)) {
                tokenList.add(rawToken);
            }
        }

        return tokenList;
    }
}
