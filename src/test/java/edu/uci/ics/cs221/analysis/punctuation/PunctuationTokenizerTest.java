package edu.uci.ics.cs221.analysis.punctuation;

import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import org.junit.Test;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PunctuationTokenizerTest {
    public PunctuationTokenizerTest() {
        System.out.println("Describe: PunctuationTokenizer");
    }
    @Test
    public void testFunctionality() {
        System.out.println("It: can tokenize normal string with white spaces");

        String text = "I am Happy Today!";
        List<String> expected = Arrays.asList("happy", "today");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testTrim() {
        System.out.println("It: should deal with string with space at the front and end");

        String text = "     I am Happy Today!      ";
        List<String> expected = Arrays.asList("happy", "today");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testAdjacentSpaces() {
        System.out.println("It: should tokenize the string with multiple adjacent white spaces");

        String text = "   I     am    Happy Today";
        List<String> expected = Arrays.asList("happy", "today");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }
}
