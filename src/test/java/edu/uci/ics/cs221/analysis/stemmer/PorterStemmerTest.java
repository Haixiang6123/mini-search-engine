package edu.uci.ics.cs221.analysis.stemmer;

import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.Stemmer;
import org.junit.Test;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

public class PorterStemmerTest {

    public static String testStem(Stemmer stemmer, String sentence) {
        return Arrays.stream(sentence.split("\\s+"))
                .map(token -> stemmer.stem(token))
                .collect(joining(" "));
    }

    @Test
    public void test0() {
        String original = "stemming is an important concept in computer science";
        String expected = "stem is an import concept in comput scienc";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test1() {
        String original = "clothes satisfactory wearing worn wore";
        String expected = "cloth satisfactori wear worn wore";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    // test2 tests if proper nouns will be stemmed or not. We expected the name and some product name will stay unchanged based on porter
    // stemmer algorithms, however, some proper nouns for example, names do have a root.
    @Test
    public void test2() {
        String original = "Intellij IDEA is so popular among programmers that my friends Tom and Jerry both use it often.";
        String expected = "Intellij IDEA is so popular among programm that my friend Tom and Jerri both us it often.";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    // test3 tests if a word is composed of two distinct words, the root form will be the adding up of the roots of the two element words or not.
    // However, the answer is no.
    @Test
    public void test3() {
        String original = "how ever however";
        String expected = "how ever howev";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    // Changing the words "writing", "Turning", "results", "applications", and "this" to their roots.
    // To test if the Stemmer can change words with added suffix and complex pattern word to their roots.
    @Test
    public void test4() {
        String original = "I am writing to test the Stemmer. Turning in the final results of the applications is due this week";
        String expected = "I am write to test the Stemmer. Turn in the final result of the applic is due thi week";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    // Changing the words "information", "retrieval", "activity", "obtaining",  "information", "resources", "relevant", and "collection".
    // To test if the Stemmer can turn complex words to their roots.
    @Test
    public void test5() {
        String original = "information retrieval is the activity of obtaining information system resources relevant to an information need from a collection";
        String expected = "inform retriev is the activ of obtain inform system resourc relev to an inform need from a collect";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    // Turning the words "fished" and "weeks" to their roots.
    // To test if the Stemmer can turn words with added suffix to their roots.
    @Test
    public void test6() {
        String original = "He is an old man who fished alone in a skiff in the Gulf Stream and he had gone twenty-two weeks without taking a fish";
        String expected = "He is an old man who fish alon in a skiff in the Gulf Stream and he had gone twenty-two week without take a fish";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test_words_which_shouldnt_be_modified() {
        /*
         Test words that should not be modified/stemmed by the stemmer
         as they are already in their "root" forms.
         */

        String original = "rate roll sky feed bled sing caress 1234";
        String expected = "rate roll sky feed bled sing caress 1234";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test_plurals() {
        /*
         Test plural words.
         Check whether words in their plural forms are converted into their singular forms by the stemmer.
         */

        String original = "caresses ponies cats";
        String expected = "caress poni cat";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test_words_with_diff_suffix() {
        /*
         Test words with different suffixes.
         Check if words with different forms of suffixes are converted to their root forms correctly.
         */

        String original = "plastered probate relational goodness hopeful feudalism motoring differently formality " +
                "defensible adjustment bowdlerize adoption operator homologous irritant";
        String expected = "plaster probat relat good hope feudal motor differ formal defens adjust bowdler adopt " +
                "oper homolog irrit";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test_empty_string() {
        /*
         Test an empty string as input; an empty string should be returned
         */

        String original = "";
        String expected = "";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test_sentence() {
        /*
         Test a generic sentence with different types of words.
         */
        String original = "Indeed, my only wonder was that he had not already been mixed up in this extraordinary case," +
                " which was the one topic of conversation through the length and breadth of England.";
        String expected = "Indeed, my onli wonder wa that he had not alreadi been mix up in thi extraordinari case," +
                " which wa the on topic of convers through the length and breadth of England.";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test7() {
        /*
        test plurals and -ed or -ing.
         */

        String original = "ties dogs caress need agreed disabled fitting making missing meeting meetings";
        String expected = "ti dog caress need agre disabl fit make miss meet meet";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test8() {
        /*
        test of taking off -ization, -izer, -tional, -ibility, -ness.
         */

        String original = "organization organizer international responsibility fitness";
        String expected = "organ organ intern respons fit";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test9() {
        /*
        test of taking off -ment, -ness, -ence, -fulness, -ical, -ism.
         */

        String original = "department humorousness dependence helpfulness analytical despotism";
        String expected = "depart humor depend help analyt despot";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    @Test
    public void test10() {

        // This test case test whether stemmer could stem words end with "ible", "ize" and "ion" as well as word in past tense.


        String original = "this wall is regarded as of the indestructible construction in ancient time which was built with"
                + " marble in standardized size and designed by smartest scientist at that time";

        String expected = "thi wall is regard as of the indestruct construct in ancient time which wa built with"
                + " marbl in standard size and design by smartest scientist at that time";


        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));


    }
    @Test
    public void test11() {

        // This test case test whether stemmer could stem words end with "fully", "ator", "ment", "ness" and "ing".


        String original = "hopefully the refrigerator start working again in that chen li made some adjustment with carefulness";

        String expected = "hopefulli the refriger start work again in that chen li made some adjust with care";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));


    }
    @Test
    public void test12() {

        // This test case test whether stemmer could stem words end with "ance", "ate", "y", "val" and "ism".


        String original = "the allowance of collaboration between media and tech company help activate the revival of journalism";

        String expected = "the allow of collabor between media and tech compani help activ the reviv of journal";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));


    }

    /**
     * This test case covers the words which should be modified (or should not be falsely modified) by step 1a, 1b,
     * 1b-cleanup, 1c of Porter Stemmer
     *
     * Covers words ending with - `sses`, `ies`, `s`, (m>0)`eed`, (*v*)`ed`, (*v*)`ing`, (*v*) y
     *
     * `agreed` becomes `agree` in step1 but changes to `agre` in step 5a of Porter Stemmer
     * `conflated` becomes `conflate` in step1 but changes to `conflat` in step 4 of Porter Stemmer
     * `troubled` becomes `trouble` in step1 but changes to `troubl` in step 5a of Porter Stemmer
     * `filing` becomes `file` in step1 but changes to `fil` in step 5a of Porter Stemmer
     */
    @Test
    public void test13() {
        String originalString1a = "caresses ponies caress cats ";
        String expectedString1a = "caress poni caress cat ";

        String originalString1b = "agreed feed plastered bled motoring sing ";
        String expectedString1b = "agre feed plaster bled motor sing ";

        String originalString1bCleanup = "conflated troubled sized hopping fizzed failing filing ";
        String expectedString1bCleanup = "conflat troubl size hop fizz fail file ";

        String originalString1c = "happy sky";
        String expectedString1c = "happi sky";

        String original = originalString1a + originalString1b + originalString1bCleanup + originalString1c;
        String expected = expectedString1a + expectedString1b + expectedString1bCleanup + expectedString1c;

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    /**
     * This test case covers the words which should be modified (or should not be falsely modified) by step 2
     * of Porter Stemmer
     *
     * covers words with (m>0) `ational`, (m>0) `tional`, (m>0) `enci`, (m>0) `anci`, (m>0) `izer`, (m>0) `abli`,
     * (m>0) `alli`, (m>0) `entli`, (m>0) `eli`, (m>0) `ousli`, (m>0) `ization`, (m>0) `ation`, (m>0) `ator`,
     * (m>0) `alism`, (m>0) `iveness`, (m>0) `fulness`, (m>0) `ousness`, (m>0) `aliti`, (m>0) `iviti`, (m>0) `biliti`
     *
     * `relational` becomes `relate` in step2 but changes to `relat` in step 5a of Porter Stemmer
     * `conditional` becomes `condition` in step2 but changes to `condit` in step 4 of Porter Stemmer
     * `rational` stays `rational` in step2 but changes to `ration` in step 4 of Porter Stemmer
     * `valenci` stays `valence` in step2 but changes to `valenc` in step 5a of Porter Stemmer
     *
     * Similarly for the remaining words
     */
    @Test
    public void test14() {
        String original = "relational conditional rational valenci hesitanci digitizer conformabli radicalli " +
                "differentli vileli analogousli vietnamization predication operator feudalism decisiveness " +
                "hopefulness callousness formaliti sensitiviti sensibiliti";
        String expected = "relat condit ration valenc hesit digit conform radic differ vile analog vietnam predic " +
                "oper feudal decis hope callous formal sensit sensibl";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    /**
     * This test case covers the words which should be modified (or should not be falsely modified) by step 3
     * of Porter Stemmer
     *
     * covers words with (m>0) `icate`, (m>0) `ative`, (m>0) `alize`, (m>0) `iciti`, (m>0) `ical`, (m>0) `ful` and
     * (m>0) `ness`,
     *
     * `electriciti` and `electrical` becomes `electric` in step3 but changes to `electr` in step 4 of Porter Stemmer
     */
    @Test
    public void test15() {
        String original = "triplicate formative formalize electriciti electrical hopeful goodness";
        String expected = "triplic form formal electr electr hope good";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    /**
     * This test case covers the words which should be modified (or should not be falsely modified) by step 4
     * of Porter Stemmer
     *
     * covers words with (m>1) `al`, (m>1) `ance`, (m>1) `ence`, (m>1) `er`, (m>1) `ic`, (m>1) `able`, (m>1) `ible`,
     * (m>1) `ant`, (m>1) `ement`, (m>1) `ment`, (m>1) `ent`, (m>1 and (*S or *T)) `ion`, (m>1) `ou`, (m>1) `ism`,
     * (m>1) `ate`, (m>1) `iti`, (m>1) `ous`, (m>1) `ive`, (m>1) `ize`
     */
    @Test
    public void test16() {
        String original = "revival  allowance inference airliner gyroscopic adjustable defensible irritant replacement " +
                "adjustment dependent adoption homologou communism activate angulariti homologous effective bowdlerize";
        String expected = "reviv allow infer airlin gyroscop adjust defens irrit replac adjust depend adopt homolog " +
                "commun activ angular homolog effect bowdler";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

    /**
     * This test case covers the words which should be modified (or should not be falsely modified) by step 5a, 5b
     * of Porter Stemmer
     *
     * covers words with (m>1) e, (m=1 and not *o) e, (m > 1 and *d and *L)
     */
    @Test
    public void test17() {
        String original = "probate rate cease controll roll";
        String expected = "probat rate ceas control roll";

        PorterStemmer porterStemmer = new PorterStemmer();
        assertEquals(expected, testStem(porterStemmer, original));
    }

}
