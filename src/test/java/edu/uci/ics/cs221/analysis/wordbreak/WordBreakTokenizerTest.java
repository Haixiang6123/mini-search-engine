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
