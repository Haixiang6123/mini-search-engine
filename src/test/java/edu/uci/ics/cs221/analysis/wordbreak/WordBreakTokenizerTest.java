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

    // Team 8
    //check if the output is lowercased and stop words are removed
    @Test
    public void team8Test1() {
        String text = "THISiswhATItoldyourI'llFRIendandI'llgoonlinecontactcan'tforget";
        List<String> expected = Arrays.asList("old", "i'll", "friend", "i'll", "go", "online", "contact", "can't", "forget");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    //check whether the work-break functions when meet strings like "whatevergreen" and the string constists of more than one frequenctly used word
    @Test
    public void team8Test2() {
        String text = "informationinforTHOUGHTFULLYcopyrightwhatevercontactablewhatevergreen";
        List<String> expected = Arrays.asList("information", "thoughtfully", "copyright", "whatever", "contact", "able", "whatever", "green");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }


    //check if the program can throw an exception when the string is unbreakable
    @Test(expected = RuntimeException.class)
    public void team8Test3() {
        String text = "$reLLL(  ghn)iog*";
        //throw exception
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);
    }

    // Team 9
    // Test1: test for empty input
    @Test
    public void team9Test1() {
        String text = "";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test2: Test for upper case input
    @Test
    public void team9Test2() {
        String text = "ILIKEINFORMATIONRETRIEVAL";
        List<String> expected = Arrays.asList("like", "information", "retrieval");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test3: for the case which has multiple answers
    // BAD: the rear eel even pine apples
    // BAD: there are eleven pine apples
    // GOOD: there are eleven pineapples
    @Test
    public void team9Test3() {
        String text = "thereareelevenpineapples";
        List<String> expected = Arrays.asList("eleven", "pineapples");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test4: invalid input, expect an exception.
    // exception type depends on the implementation.
    @Test(expected = RuntimeException.class)
    public void team9Test4() {
        String text = "abc123";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);
    }

    // Team 10
    @Test
    public void tema10Test1() {
        String text = "Itisnotourgoal";
        // Original: "It is not our goal"
        // we didn't use "It's" because "it's" is not in the provided dictionary.
        // It's easy to be broken into "it is no tour goal", which is false.

        List<String> expected = Arrays.asList("goal");
        // "it" "is" "not" "our" are all stop words, which should be discarded.
        // False: {"tour", "goal"}

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void team10Test2() {
        String text = "FindthelongestpalindromicstringYoumayassumethatthemaximumlengthisonehundred";
        // Original: "Find the longest palindromic string. You may assume that the maximum length is one hundred."
        // Test if the WordBreaker is efficient enough to handle long complex strings correctly.

        List<String> expected = Arrays.asList("find", "longest", "palindromic", "string", "may",
                "assume", "maximum", "length", "one", "hundred");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Team 11
    @Test
    public void team11Test1() {
        String text = "tobeornottobe";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Empty input test
    @Test
    public void team11Test2() {
        String text = "";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Input string cannot be tokenized, an UnsupportedOperationException is expected
    @Test(expected = RuntimeException.class)
    public void team11Test3() {
        String text = "b";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);
        // An exception should be thrown above, and the program should never reach the next line
        assert (false);
    }

    // Test with a normal case: it can be tokenized with the highest probability
    @Test
    public void team11Test4() {
        String text = "searchnewtimeuse";
        List<String> expected = Arrays.asList("search", "new", "time", "use");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test with a few upper cases
    @Test
    public void team11Test5() {
        String text = "seaRchneWtiMeuSe";
        List<String> expected = Arrays.asList("search", "new", "time", "use");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test with all upper cases
    @Test
    public void team11Test6() {
        String text = "SEARCHNEWTIMEUSE";
        List<String> expected = Arrays.asList("search", "new", "time", "use");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test with stop word at the beginning
    @Test
    public void team11Test7() {
        String text = "thesearchnewtimeuse";
        List<String> expected = Arrays.asList("search", "new", "time", "use");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test with stop word in the middle
    @Test
    public void team11Test8() {
        String text = "searchthenewtimeuse";
        List<String> expected = Arrays.asList("search", "new", "time", "use");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Test with stop word in the end
    @Test
    public void team11Test9() {
        String text = "searchnewtimeusethe";
        List<String> expected = Arrays.asList("search", "new", "time", "use");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // Team 12
    //below test tests removing stop words
    @Test
    public void team12Test1() {
        String text = "thelordofthering";
        List<String> expected = Arrays.asList("lord", "ring");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    //this test case checks if word is capitalized, should be lowered and also checks removal of stop
    //words as well as returning peanut butter instead of pea, nut, but, ter which are also in the
    //dictionary
    @Test
    public void team12Test2() {
        String text = "IWANTtohavepeanutbuttersandwich";
        List<String> expected = Arrays.asList("want", "peanut", "butter", "sandwich");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    //this test checks that when text has a word not in dictionary and has spaces and question mark,
    //then it should throw an exception
    @Test(expected = RuntimeException.class)
    public void team12Test3() {
        String text = "Where did Ghada go?";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);
    }

    // Team 14
    /*
        The purpose of test one is to see if when input string can
        not be broken into tokens a runtime exception is throw per
        the last requirement of task 2. For the given input string if
        the tokenizer breaks the first token to be fra which is in the
        dictionary. There are no words in the dictionary that are lpr.
        And there are no words that have just pr or prt. So this should
        be an example where the word breaker tokenizer fails.
     */
    @Test(expected = RuntimeException.class)
    public void team14Test1() {
        String text = "fralprtnqela";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);

    }

    /*
     In the second test we want to test the ability to find matches
     that are lower cased and the ability to remove the stop words.
     In order to do this we are creating a sentence that
     */
    @Test
    public void team14Test2() {
        String text = "WEhaveaCOOLTaskinFrontOfUSANDwEShouldbehavingAgoodTIme";
        List<String> expected = Arrays.asList("cool", "task", "front", "us", "behaving", "good", "time");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    @Test(expected = RuntimeException.class)
    public void team14Test3() {
        String text = "WhatHappensWhenWeaddAperiod.";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);

    }

    @Test(expected = RuntimeException.class)
    public void team14Test4() {
        String text = "This is too check if an exception is thrown when there are spaces";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);

    }

    // Team 17
    /*
     * The test1() is to check that the "san francisco" is tokenized as one single token instead of two separate tokens.
     * If there are two separate tokens "san" and "francisco" then the meaning of the input string changes drastically.
     * Additionally, the upper case characters must be converted to lower case.
     * */
    @Test
    public void team17Test1() {
        String text = "IlOveSAnFrancIsCo";
        List<String> expected = Arrays.asList("love", "san", "francisco");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /*
     * The test2() has only the stopwords.
     * Hence the output must be an empty array and should not throw any exception as the string can broken into
     * words that exist in the dictionary.
     * Additionally, this testcase also tests the program to handle a really long string.
     * */

    @Test
    public void team17Test2() {
        String text = "imemymyselfweouroursourselvesyouyouryoursyourselfyourselveshehimhishimselfsheherhersherselfititsitselftheythemtheirtheirsthemselveswhatwhichwhowhomthisthatthesethoseamisarewaswerebebeenbeinghavehashadhavingdodoesdiddoing";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    /*
     * The test3() has an empty string
     * Hence the wordbreak tokenizer must return empty set of tokens
     * */
    @Test
    public void team17Test3() {
        String text = "";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    /*
     * The test4() has string with punctuations
     * Hence the wordbreak tokenizer must throw an exception
     * */
    @Test(expected = RuntimeException.class)
    public void team17Test4() {
        String text = "mother-in-law";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);
    }

    /*
     * The test5() has two possible set of tokens
     * But the wordbreak tokenizer must come up with the most optimal one that is ["hello", "range"] instead of ["hell", "orange"]
     * */
    @Test
    public void team17Test5() {
        String text = "hellorange";
        List<String> expected = Arrays.asList("hello", "range");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text)); //must expect an exception

    }

    // Team 4
    @Test
    public void testCanBreak() {
        System.out.println("It: can break normal string");

        String text = "catdog";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testChinese() {
        System.out.println("It: can break string with Chinese");

        String text = "你好我是一个人";
        List<String> expected = Arrays.asList("你好", "我", "是", "一个", "人");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testJapanese() {
        System.out.println("It: can break string with Japanese");

        String text = "さようなら友達";
        List<String> expected = Arrays.asList("さようなら", "友達");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testDuplication() {
        System.out.println("It: can deal with duplicate sub string");

        String text = "catdogcatdog";
        List<String> expected = Arrays.asList("cat", "dog", "cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test(expected = RuntimeException.class)
    public void testCanNotBreak() {
        System.out.println("It: can not break string which is not in dictionary");

        String text = "xzy";
        List<String> expected = new ArrayList<>();

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test(expected = RuntimeException.class)
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

    @Test
    public void testContainStopWord() {
        System.out.println("It: should not contain STOP WORDS result");

        String text = "mecatdog";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void testProbCompare() {
        System.out.println("It: should have higher probability tokens");

        String text = "something";
        List<String> expected = Arrays.asList("something");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    // test a long text, with 20 seconds timeout
    @Test(timeout=20000)
    public void longTest1() {
        String text = "tosherlockholmessheisalwaysthewomanihaveseldomheardhimmentionherunderanyothernameinhiseyessheeclipsesandpredominatesthewholeofhersexitwasnotthathefeltanyemotionakintoloveforireneadlerallemotionsandthatoneparticularlywereabhorrenttohiscoldprecisebutadmirablybalancedmindhewasitakeitthemostperfectreasoningandobservingmachinethattheworldhasseenbutasaloverhewouldhaveplacedhimselfinafalsepositionheneverspokeofthesofterpassionssavewithagibeandasneertheywereadmirablethingsfortheobserverexcellentfordrawingtheveilfrommenmotivesandactionsbutforthetrainedreasonertoadmitsuchintrusionsintohisowndelicateandfinelyadjustedtemperamentwastointroduceadistractingfactorwhichmightthrowadoubtuponallhismentalresultsgritinasensitiveinstrumentoracrackinoneofhisownhighpowerlenseswouldnotbemoredisturbingthanastrongemotioninanaturesuchashisandyettherewasbutonewomantohimandthatwomanwasthelateireneadlerofdubiousandquestionablememory";
        String expectedStr = "sherlock holmes always woman seldom heard mention name eyes eclipses predominates whole sex felt emotion akin love irene adler emotions one particularly abhorrent cold precise admirably balanced mind take perfect reasoning observing machine world seen lover would placed false position never spoke softer passions save gibe sneer admirable things observer excellent drawing veil men motives actions trained reasoner admit intrusions delicate finely adjusted temperament introduce distracting factor might throw doubt upon mental results grit sensitive instrument crack one high power lenses would disturbing strong emotion nature yet one woman woman late irene adler dubious questionable memory";
        List<String> expected = Arrays.asList(expectedStr.split(" "));

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));

    }

    // test a long text, with 20 seconds timeout
    @Test(timeout=20000)
    public void longTest2() {
        String text = "ihadseenlittleofholmeslatelymymarriagehaddriftedusawayfromeachothermyowncompletehappinessandthehomecentredinterestswhichriseuparoundthemanwhofirstfindshimselfmasterofhisownestablishmentweresufficienttoabsorballmyattentionwhileholmeswholoathedeveryformofsocietywithhiswholesoulremainedinourlodgingsinbakerstreetburiedamonghisoldbooksandalternatingfromweektoweekbetweencocaineandambitionthedrowsinessofthedrugandthefierceenergyofhisownkeennaturehewasstillaseverdeeplyattractedbythestudyofcrimeandoccupiedhisimmensefacultiesandextraordinarypowersofobservationinfollowingoutthosecluesandclearingupthosemysterieswhichhadbeenabandonedashopelessbytheofficialpolicefromtimetotimeiheardsomevagueaccountofhisdoingsofhissummonstoodessainthecaseofthemurderofhisclearingupofthesingulartragedyoftheatkinsonbrothersattrincomaleeandfinallyofthemissionwhichhehadaccomplishedsodelicatelyandsuccessfullyforthereigningfamilyofhollandbeyondthesesignsofhisactivityhoweverwhichimerelysharedwithallthereadersofthedailypressiknewlittleofmyformerfriendandcompanion";
        String expectedStr = "seen little holmes lately marriage drifted us away complete happiness home centred interests rise around man first finds master establishment sufficient absorb attention holmes loathed every form society whole soul remained lodgings baker street buried among old books alternating week week cocaine ambition drowsiness drug fierce energy keen nature still ever deeply attracted study crime occupied immense faculties extraordinary powers observation following clues clearing mysteries abandoned hopeless official police time time heard vague account doings summons odessa case murder clearing singular tragedy atkinson brothers trincomalee finally mission accomplished delicately successfully reigning family holland beyond signs activity however merely shared readers daily press knew little former friend companion";
        List<String> expected = Arrays.asList(expectedStr.split(" "));

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));

    }
}
