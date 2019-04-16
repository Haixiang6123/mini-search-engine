package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.JapaneseTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JapaneseTokenizerTest {
    //はリンゴを食べる
    @Test
    public void test1() {
        System.out.println("It: can break string with Japanese");

        String text = "さようなら友達";
        List<String> expected = Arrays.asList("さようなら", "友達");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test2() {
        System.out.println("It: can break string with Japanese");

        String text = "ジンボはリンゴを食べる";
        List<String> expected = Arrays.asList("ジン","ボ","は","リンゴ","を","食べる");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test3() {
        System.out.println("It: can break string with Japanese");

        String text =   "あなたを愛しています";
        List<String> expected = Arrays.asList("あなた", "を", "愛し", "て", "い", "ます");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }


}
