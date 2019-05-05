package edu.uci.ics.cs221.inverted;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.NaiveAnalyzer;
import edu.uci.ics.cs221.storage.Document;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class Team4OrSearchTest {
    private InvertedIndexManager manager = null;
    // Initialize document
    private Document doc1 = new Document("cat dog cat dog");
    private Document doc2 = new Document("apple dog");
    private Document doc3 = new Document("cat smile");
    private String indexFolderName = "index";
    private String teamFolderName = "Team18FlushTest";

    @Before
    public void before() {
        // Initialize analyzer
        Analyzer analyzer = new NaiveAnalyzer();
        // Initialize InvertedIndexManager
        manager = InvertedIndexManager.createOrOpen(Paths.get(indexFolderName, teamFolderName).toString(), analyzer);
        manager.addDocument(doc1);
        manager.addDocument(doc2);
        manager.addDocument(doc3);
        // Flush to disk
        manager.flush();
    }

    @Test
    public void test0() {
        manager.mergeAllSegments();
    }

    /**
     * Test for normal search or case
     * This test case is going to search "cat" or "apple"
     * The result should be doc1, doc2, doc3
     */
//    @Test
    public void test1() {
        // Generate expected list
        List<Document> expected = Arrays.asList(doc1, doc2, doc3);

        // Generate keywords
        List<String> keywords = Arrays.asList("cat", "apple");

        // Make query
        Iterator<Document> results = manager.searchOrQuery(keywords);

        // Assertion
        for (int i = 0; results.hasNext(); i++) {
            assertEquals(results.next().getText(), expected.get(i).getText());
        }
    }

    /**
     * Test for empty keyword
     * Result should be an empty list of Documents
     */
    @Test
    public void test2() {
        // Generate keywords
        List<String> keywords = Arrays.asList("");

        // Make query
        Iterator<Document> results = manager.searchOrQuery(keywords);

        // Assertion
        assertFalse(results.hasNext());
    }

    /**
     * Test for punctuation characters
     * Results should be an empty list of Documents
     */
   @Test
    public void test3() {
        // Generate keywords
        List<String> keywords = Arrays.asList(",", ":./");

        // Make query
        Iterator<Document> results = manager.searchOrQuery(keywords);

        // Assertion
        assertFalse(results.hasNext());
    }


    /**
     * Clean up the cache files
     */
//    @After
    public void after() {
        File indexFolder = new File(Paths.get(indexFolderName).toString());
        File teamFolder = new File(Paths.get(indexFolderName, teamFolderName).toString());
        // Delete files
        for (File file : Objects.requireNonNull(teamFolder.listFiles())) {
            try {
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // Delete folders
        if (teamFolder.exists()) {
            teamFolder.delete();
        }
        if (indexFolder.exists()) {
            indexFolder.delete();
        }
    }
}