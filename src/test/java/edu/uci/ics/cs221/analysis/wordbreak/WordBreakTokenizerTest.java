package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordBreakTokenizerTest {

    public WordBreakTokenizerTest() {
        System.out.println("Describe: WordBreakTokenizer");
    }

    @Test
    public void testCanBreak() {
        System.out.println("It: can break normal string");

        String text = "catdog";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testCanNotBreak() {
        System.out.println("It: can not break string which is not in dictionary");

        String text = "xzy";
        List<String> expected = new ArrayList<>();

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testInvalidCharacter() {
        System.out.println("It: can deal with string with invalid character");

        String text = "!@#$$";
        List<String> expected = new ArrayList<>();

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testUppercaseString() {
        System.out.println("It: has lowercase result");

        String text = "CATDOG";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testNotTrimString() {
        System.out.println("It: can deal with not-trim string");

        String text = "       catdog     ";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testEmptyString() {
        System.out.println("It: can deal with empty string");

        String emptyText = "";
        String nullText = null;
        List<String> expected = new ArrayList<>();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(emptyText));
        assertEquals(expected, tokenizer.tokenize(nullText));
    }
}
