package edu.uci.ics.cs221.analysis;

import utils.Utils;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 *
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 *
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 *
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 *
 * Requirements:
 *  - Use Dynamic Programming for efficiency purposes.
 *  - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 *      The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 *  - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 *  - Stop words should be removed.
 *  - If there's no possible way to break the string, throw an exception.
 *
 */
public class WordBreakTokenizer implements Tokenizer {
    private Map<String, Long> dict = null;
    private Set<String> dictTokens = null;

    class Result {
        public boolean breakable = false;
        public List<String> tokens = new ArrayList<>();

        @Override
        public String toString() {
            return "" + breakable + " " + Utils.stringifyList(tokens);
        }
    }

    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            List<String> lines = Files.readAllLines(Paths.get(dictResource.toURI()));
            // Get dictionary
            initDict(lines);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initDict(List<String> lines) {
        double total = 0;
        dict = new HashMap<>();
        for (String line : lines) {
            // Init item
            String[] item = line.trim().split(" ");
            dict.put(item[0], Double.valueOf(item[1]));
            // Compute total
            total += Double.valueOf(item[1]);
            // Prevent from empty string
            if (item.length != 2) {
                continue;
            }
            dict.put(item[0], Long.valueOf(item[1]));
        }
        dictTokens = dict.keySet();

        // Compute probabilities for each token
        getProbabilities(total);
    }

    private void getProbabilities(double total) {
        for (String token : dict.keySet()) {
            double frequency = dict.get(token);
            dict.put(token, frequency / total);
        }
    }

    public List<String> tokenize(String text) {
        // Deal with empty string
        if (text == null || text.equals("")) {
            return new ArrayList<>();
        }
        // Pre-process text
        text = text.trim().toLowerCase();
        int n = text.length();

        Result[][] matrix = new Result[n][n];

        // n: text length
        // Window size: 1 to n
        for (int window = 1; window <= n; window++) {
            // Start index: 0 to n - window size
            for (int start = 0; start <= n - window; start++) {
                Result result = new Result();
                int end = start + window - 1;
                String subText = text.substring(start, end + 1);

                if (dictTokens.contains(subText)) {
                    result.breakable = true;
                    result.tokens.add(subText);
                }
                else {
                    for (int middle = start; middle < end; middle++) {
                        if (isBreakable(matrix, start, middle, end)) {
                            Result left = matrix[start][middle];
                            Result right = matrix[middle + 1][end];
                            result.tokens.addAll(left.tokens);
                            result.tokens.addAll(right.tokens);
                            result.breakable = true;
                            // todo: need to consider the probability
                            break;
                        }
                    }
                }
                matrix[start][end] = result;
            }
        }
        return matrix[0][n - 1].tokens;
    }

    // Check if it can be broken it by given start, end index
    private boolean isBreakable(Result[][] store, int start, int middle, int end) {
        return store[start][middle].breakable
                && store[middle + 1][end].breakable;
    }
}