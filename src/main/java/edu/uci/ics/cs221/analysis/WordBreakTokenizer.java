package edu.uci.ics.cs221.analysis;

import utils.Utils;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 * <p>
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 * <p>
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 * <p>
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 * <p>
 * Requirements:
 * - Use Dynamic Programming for efficiency purposes.
 * - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 * The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 * - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 * - Stop words should be removed.
 * - If there's no possible way to break the string, throw an exception.
 */
public class WordBreakTokenizer implements Tokenizer {
    private Map<String, Double> dict = new HashMap<>();
    private Set<String> dictTokens = null;

    class Result {
        public boolean breakable = false;
        public List<String> tokens = new ArrayList<>();
        public double probability = 0;

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

    // Init a dictionary for all tokens
    private void initDict(List<String> lines) {
        double total = 0;
        for (String line : lines) {
            // Init item
            String[] item = line.trim().split(" ");
            if (item.length != 2) {
                continue;
            }
            dict.put(item[0], Double.valueOf(item[1]));
            // Compute total
            total += Double.valueOf(item[1]);
            dict.put(item[0], Double.valueOf(item[1]));
        }
        dictTokens = dict.keySet();

        // Compute probabilities for each token
        getProbabilities(total);
    }

    // Init probabilities from given resource file
    private void getProbabilities(double total) {
        for (String token : dict.keySet()) {
            double frequency = dict.get(token);
            dict.put(token, frequency / total);
        }
    }

    // Main method for tokenize words
    public List<String> tokenize(String text) {
        // Deal with empty string
        if (text == null || text.equals("")) {
            return new ArrayList<>();
        }
        Result[][] matrix = new Result[text.length()][text.length()];

        // Pre-process text
        text = text.trim().toLowerCase();

        // Break pre-processed text
        breakWord(matrix, text);

        // Final result: 0 -> text length - 1
        Result result = matrix[0][text.length() - 1];
        if (!result.breakable) {
            // todo: throw error
            System.out.println("Throw Error");
        }
        // Filter stop words
        result.tokens = filterStopWords(result.tokens);
        return result.tokens;
    }

    // Start to break word
    private void breakWord(Result[][] matrix, String text) {
        int n = text.length();
        for (int window = 1; window <= n; window++) {
            // Start index: 0 -> n - window size
            for (int start = 0; start <= n - window; start++) {
                int end = start + window - 1;
                String subText = text.substring(start, end + 1);
                Result result = new Result();

                // Check sub text whether exist in dictionary
                checkDict(result, subText, dict);

                // Compare with previous sub text probabilities
                comparePrevSubText(result, start, end, matrix);

                matrix[start][end] = result;
            }
        }
    }

    // Check if sub text exist in dict
    private void checkDict(Result result, String subText, Map<String, Double> dict) {
        if (dictTokens.contains(subText)) {
            result.breakable = true;
            result.tokens.add(subText);
            // Init probability for this sub text
            result.probability = dict.get(subText);
        }
    }

    // Compare current sub text probability with previous sub texts
    private void comparePrevSubText(Result result, int start, int end, Result[][] matrix) {
        for (int middle = start; middle < end; middle++) {
            if (isBreakable(matrix, start, middle, end)) {
                Result left = matrix[start][middle];
                Result right = matrix[middle + 1][end];
                double curProb = left.probability * right.probability;
                if (curProb > result.probability) {
                    updateResult(result, left, right, curProb);
                }
            }
        }
    }

    // Remove STOP WORDS from result list
    private List<String> filterStopWords(List<String> tokens) {
        List<String> filteredList = new ArrayList<>();
        for (String token : tokens) {
            if (!StopWords.stopWords.contains(token)) {
                filteredList.add(token);
            }
        }

        return filteredList;
    }

    private void updateResult(Result result, Result left, Result right, double curProb) {
        result.probability = curProb;
        result.tokens.clear();
        result.tokens.addAll(left.tokens);
        result.tokens.addAll(right.tokens);
        result.breakable = true;
    }

    // Check if it can be broken it by given start, end index
    private boolean isBreakable(Result[][] store, int start, int middle, int end) {
        return store[start][middle].breakable
                && store[middle + 1][end].breakable;
    }
}