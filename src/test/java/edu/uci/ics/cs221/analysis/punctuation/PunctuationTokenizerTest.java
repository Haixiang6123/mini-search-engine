package edu.uci.ics.cs221.analysis.punctuation;

import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.analysis.StopWords;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PunctuationTokenizerTest {
    public PunctuationTokenizerTest() {
        System.out.println("Describe: PunctuationTokenizer");
    }

    // Team 1
    /**
     * Test whether the punctuation tokenizer handles white spaces correctly.
     *
     * Note: a whitespace character is assumed to be one of:
     *   1. ' ' (normal space, Unicode codepoint U+0020, ASCII code 32);
     *   2. '\t' (horizontal tab, Unicode codepoint U+0009, ASCII code 9);
     *   3. '\n' (line feed, Unicode codepoint U+000A, ASCII code 10).
     * Other space-like characters or Unicode whitespace characters are not
     * considered, unless the project specification further indicates otherwise.
     *
     * Test text:       {@code "uci cs221\tinformation\nretrieval"}
     * Expected tokens: {@code ["uci", "cs221", "information", "retrieval"]}
     */
    @Test
    public void whiteSpacesShouldDelimit() {
        String text = "uci cs221\tinformation\nretrieval";
        List<String> expected = Arrays.asList("uci", "cs221", "information", "retrieval");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer handles punctuations correctly.
     *
     * Note: a punctuation delimiter is assumed to be one of ',', '.', ';', '?', and '!',
     * @see PunctuationTokenizer#punctuations
     *
     * Test text:       {@code "uci,cs221.information;retrieval?project!1"}
     * Expected tokens: {@code ["uci", "cs221", "information", "retrieval", "project", "1"]}
     */
    @Test
    public void punctuationsShouldDelimit() {
        String text = "uci,cs221.information;retrieval?project!1";
        List<String> expected =
                Arrays.asList("uci", "cs221", "information", "retrieval", "project", "1");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer handles non-delimiter characters correctly.
     *
     * Test text:       {@code "uci~cs221/information>retrieval"}
     * Expected tokens: {@code ["uci~cs221/information>retrieval"]}
     */
    @Test
    public void nonDelimiterShouldNotDelimit() {
        String text = "uci~cs221/information>retrieval";
        List<String> expected = Arrays.asList("uci~cs221/information>retrieval");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer converts tokens into lower cases.
     *
     * Test text:       {@code "UciCS221InformationRetrieval"}
     * Expected tokens: {@code ["ucics221informationretrieval"]}
     */
    @Test
    public void tokensShouldBeLowerCase() {
        String text = "UciCS221InformationRetrieval";
        List<String> expected = Arrays.asList("ucics221informationretrieval");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer correctly handles consecutive whitespace
     * characters.
     *
     * Test text:       {@code "uci \tcs221\t\ninformation\n \tretrieval"}
     * Expected tokens: {@code ["uci", "cs221", "information", "retrieval"]}
     */
    @Test
    public void consecutiveWhiteSpacesShouldDelimit() {
        String text = "uci \tcs221\t\ninformation\n \tretrieval";
        List<String> expected = Arrays.asList("uci", "cs221", "information", "retrieval");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer correctly handles consecutive punctuations.
     *
     * Test text:       {@code "uci,.cs221.;information;?retrieval?!project!,.1"}
     * Expected tokens: {@code ["uci", "cs221", "information", "retrieval", "project", "1"]}
     */
    @Test
    public void consecutivePunctuationsShouldDelimit() {
        String text = "uci,.cs221.;information;?retrieval?!project!,.1";
        List<String> expected =
                Arrays.asList("uci", "cs221", "information", "retrieval", "project", "1");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer correctly handles leading and trailing
     * whitespace characters.
     *
     * Test text:       {@code " \t\nucics221informationretrieval \t\n"}
     * Expected tokens: {@code ["ucics221informationretrieval"]}
     */
    @Test
    public void surroundingWhiteSpacesShouldBeRemoved() {
        String text = " \t\nucics221informationretrieval \t\n";
        List<String> expected = Arrays.asList("ucics221informationretrieval");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer correctly handles leading and trailing
     * punctuations.
     *
     * Test text:       {@code ",.;?!ucics221informationretrieval,.;?!"}
     * Expected tokens: {@code ["ucics221informationretrieval"]}
     */
    @Test
    public void surroundingPunctuationsShouldBeRemoved() {
        String text = ",.;?!ucics221informationretrieval,.;?!";
        List<String> expected = Arrays.asList("ucics221informationretrieval");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /**
     * Test whether the punctuation tokenizer correctly handles stop words.
     *
     * @see edu.uci.ics.cs221.analysis.StopWords#stopWords
     */
    @Test
    public void stopWordsShouldBeRemoved() {
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        List<String> expected = Arrays.asList();

        for (String stopWord: StopWords.stopWords) {
            assertEquals(expected, tokenizer.tokenize(stopWord));
        }
    }

    /**
     * Integrated test case to check whether the punctuation tokenizer jointly
     * satisfies all requirements tested above.
     *
     * Test text:       {@code " Do UCI CS221:\tInformation Retrieval, project 1 by yourself.\n"}
     * Expected tokens: {@code ["uci", "cs221:", "information", "retrieval", "project", "1"]}
     */
    @Test
    public void integrationTest() {
        String text = " Do UCI CS221:\tInformation Retrieval, project 1 by yourself.\n";
        List<String> expected = Arrays.asList(
                "uci",
                "cs221:",
                "information",
                "retrieval",
                "project",
                "1"
        );
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Team 2
    /**
     *  test1 tests whether multiple newlines, tabs,together with spaces works
     */
    @Test
    public void team2Test1() {
        String text = "UCI: \n \n a public research university located in Irvine, \t \t California!";
        List<String> expected = Arrays.asList("uci:", "public", "research", "university", "located","irvine","california");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("check whether multiple newlines and tabs works",expected, tokenizer.tokenize(text));
    }

    /**
     *  test2 tests whether the punctuation tokenizer can identify some emojis and save them as a token
     *  test string: 🐴 is a very cute horse head!
     */
    @Test
    public void team2Test2() {
        String text = "\uD83D\uDC34 is a very cute horse head!";
        List<String> expected = Arrays.asList("\uD83D\uDC34", "cute", "horse", "head");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("check whether the tokenizer can deal with emojis" ,expected, tokenizer.tokenize(text));
    }

    /**
     *  test3: this test tests whether the punctuation tokenizer can deal with consecutive punctuations
     */
    @Test
    public void team2Test3() {
        String text = "UCI : a, public research university located in Irvine,California!!!!";
        List<String> expected = Arrays.asList("uci", ":", "public", "research", "university", "located","irvine","california");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("check consecutive punctuation works",expected, tokenizer.tokenize(text));
    }

    // Team 3
    @Test
    public void team3TestCase1(){
        String text = "Good morning, Sara!";
        List<String> expected = Arrays.asList("good", "morning","sara");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }
    //Our testcase 2
    @Test
    public void team3TestCase2(){
        String text = "Information Retrival is      the best course in UCI!";
        List<String> expected = Arrays.asList("information", "retrival","best","course","uci");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }
    @Test
    public void team3TestCase3(){
        String text = "Information Retrival is \t \n the best course in UCI!";
        List<String> expected = Arrays.asList("information", "retrival","best","course","uci");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Team 4
    @Test
    public void testCanTokenize() {
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

    @Test
    public void testEmptyString() {
        System.out.println("It: can deal with empty string");

        String emptyText = "";
        String nullText = null;
        List<String> expected = new ArrayList<>();
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals(expected, tokenizer.tokenize(emptyText));
        assertEquals(expected, tokenizer.tokenize(nullText));
    }

    // Team 5
    @Test
    public void test1() {
        String text = "He did not pass The Exam, did he?\n\r\t";
        List<String> expected = Arrays.asList("pass", "exam");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("test ,? split, ignoring stop words and uppercase to lowercase transfer",
                expected, tokenizer.tokenize(text));
    }

    @Test
    public void test2() {
        String text = "Thanks God! I found my wallet there.";
        List<String> expected = Arrays.asList("thanks", "god","found","wallet");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals("test ! . split, ignoring stop words and uppercase to lowercase transfer",
                expected, tokenizer.tokenize(text));
    }

    @Test
    public void test3() {
        String text = "";
        List<String> expected = Arrays.asList();
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("test empty input",
                expected, tokenizer.tokenize(text));
    }
    @Test
    public void test4() {
        String text = "         ";
        List<String> expected = Arrays.asList();
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals("test more  one spaces",
                expected, tokenizer.tokenize(text));
    }
    @Test
    public void test5() {
        String text = "    tomorrow";
        List<String> expected = Arrays.asList("tomorrow");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals("test more than one spaces between words",
                expected, tokenizer.tokenize(text));
    }
    @Test
    public void test6() {
        String text = "!,";
        List<String> expected = Arrays.asList();
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals("test only punctuations",
                expected, tokenizer.tokenize(text));
    }

    @Test
    public void test7() {
        String text = "Dog like Cat";
        List<String> expected = Arrays.asList("dog", "like", "cat");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("test no stop words",
                expected, tokenizer.tokenize(text));
    }

    @Test
    public void test8() {
        String text =
                "herselF, me Own. ourS; tHiS? her thEirs were onLY; THese. Hidden oUrselVeS again, agaInsT hAs An? " +
                        "our have, he. oN. bEing aM CAn WiTh; So THRough? Them tHoSe. few. itS! Below! was? once Do Is! By of eACh. " +
                        "hImself; hiM; such? My; whO haViNg beEN haD She during! bEcAuse; other doEs; uNDeR oveR sHoUld JUSt! MoRe fOr Be " +
                        "into dID WHich thE, MySelf. hers; wHErE? They; now veRy aBouT NO information bUt tHemSeLVEs aRe hOw? tHeir NoT, bEFOrE? ANd wHat " +
                        "yourself; We froM? nor yOuR aboVe too wHY Or! yOurSelVeS theRE. DOn! dOwN; T. I sAme hERE uP; At. furThEr To; While; wILL; " +
                        "yours! bEtween? ThAt. you OfF theN as aLL both? uNTil; aNY Doing? tHAn iTsELf, ouT! WhEn IT whom; S, Some most A if. iN hIs! after.";

        List<String> expected = Arrays.asList("hidden", "information");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("test all stop words and punctuations: ",
                expected, tokenizer.tokenize(text));
    }

    @Test
    public void test9() {
        String text = "I don't like your! You are a fast man!";
        List<String> expected = Arrays.asList("don't", "like","fast","man");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();

        assertEquals("test not punctuations that should not be removed.",
                expected, tokenizer.tokenize(text));
    }

    // Team 6
    @Test
    public void team6Test1() {
        /*
        Test Case 1 is to test if the tokenizer handles different
        white spaces correctly by having characters of \t, \n and white spaces in the input text.
        */
//        System.out.printf("test case 1\n");

        String text = " testcase\tgood example\nyes great example\n";
        List<String> expected = Arrays.asList("testcase", "good", "example",
                "yes", "great", "example");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void team6Test2() {
        /*
        Test Case 2 is to test if the tokenizer can handles splits the text by given punctuations marks
        and removes them correctly. The punctuations include ",", ".", ";", "?" and "!". Moreover,
        punctuation marks that are not on the list should not be considered, such as i'am and four-year-old.
         */
//        System.out.println("test case 2\n");

        String text = "Word LOL means Laughing. WHO";
        List<String> expected = Arrays.asList("word", "lol", "means", "laughing");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void team6Test3() {
        /*
        Test Case 3 is to test if the tokenizer can convert all tokens into lower case.
        */
//        System.out.println("test case 3\n");

        String text = "good, four-year-old children. never asia come? it's china! thanks.";
        List<String> expected = Arrays.asList("good", "four-year-old", "children", "never", "asia",
                "come", "it's", "china", "thanks");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void team6Test4() {
        /*
        Test Case 4 is to test if the tokenizer can filter out the stop words.
         */
//        System.out.println("test case 4\n");

        String text = "I cannot decide which car I like best " +
                "the Ferrari, with its quick acceleration and " +
                "sporty look; the midsize Ford Taurus, with " +
                "its comfortable seats and ease of handling; " +
                "or the compact Geo, with its economical fuel consumption.";
        List<String> expected = Arrays.asList("cannot", "decide", "car", "like", "best",
                "ferrari", "quick", "acceleration", "sporty", "look", "midsize", "ford",
                "taurus", "comfortable", "seats", "ease", "handling", "compact", "geo", "economical",
                "fuel", "consumption");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Team 7
        /*
        Test 1:
            Check if the tokenizer can tokenize the input string with empty space and punctuations, and
            convert the tokens to lower case.
     */
    @Test
    public void team7test01(){
        String input = " HELLO,WORLD ! ";
        List<String> output = Arrays.asList("hello","world");
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(output, tokenizer.tokenize(input));
    }

    /*
        Test 2:
            Check if the tokenizer can deal with the empty string that only contains the space, table, and
            new line mark.
     */
    @Test
    public void team7test02(){
        String input = "\n \t";
        List<String> output = new ArrayList<>();
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        assertEquals(output, tokenizer.tokenize(input));
    }
}
