package edu.uci.ics.cs221.inverted;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

/**
 * For teams doing project 2 extra credits (deletion), please add all your own deletion test cases in this class.
 * The TA will only look at this class to give extra credit points.
 */
public class InvertedIndexDeletionTest {
    private String FOLDER = "./index/Team4DeleteTest";
    private Analyzer analyzer;
    private InvertedIndexManager index;
    private Document[] documents = new Document[] {
            new Document("import edu uci ics cs221 analysis  Analyzer"),
            new Document("import edu uci ics cs221 analysis  ComposableAnalyzer"),
            new Document("import edu uci ics cs221 analysis  PorterStemmer"),
            new Document("import edu uci ics cs221 analysis  PunctuationTokenizer"),
            new Document("import edu uci ics cs221 index     inverted            InvertedIndexManager"),
            new Document("import edu uci ics cs221 storage   Document")
    };

    @Before
    public void before() {
        analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
        index = InvertedIndexManager.createOrOpen(FOLDER, analyzer);
    }

    /**
     * Test deletion when do merging
     */
    @Test
    public void testDeletionWhenMerge() {
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1;
        InvertedIndexManager.DEFAULT_MERGE_THRESHOLD = 2;

        index.addDocument(documents[0]);
        index.deleteDocuments("import");
        index.addDocument(documents[1]);
        int expectedNumSegments = 1;
        InvertedIndexSegmentForTest test = index.getIndexSegment(0);

        assertNull(test.getInvertedLists().get("import"));
        assertEquals(expectedNumSegments, index.getNumSegments());
    }

    /**
     * Test deletion for searchKeyword operation
     */
    @Test
    public void testDeletionForSearch() {
        index.addDocument(documents[0]);
        index.flush();
        index.addDocument(documents[1]);
        index.flush();
        index.addDocument(documents[2]);
        index.flush();

        Iterator<Document> beforeDeleteIterator = index.searchQuery("import");

        assertEquals(true, beforeDeleteIterator.hasNext());

        index.deleteDocuments("import");

        Iterator<Document> afterDeleteIterator = index.searchQuery("import");

        assertEquals(false, afterDeleteIterator.hasNext());
    }

    /**
     * Test deletion for searchAnd operation
     */
    @Test
    public void testDeletionForSearchAnd() {
        index.addDocument(documents[0]);
        index.flush();
        index.addDocument(documents[1]);
        index.flush();
        index.addDocument(documents[2]);
        index.flush();

        Iterator<Document> beforeDeleteIterator = index.searchAndQuery(Arrays.asList("import", "edu"));

        assertEquals(true, beforeDeleteIterator.hasNext());

        index.deleteDocuments("import");
        index.deleteDocuments("edu");

        Iterator<Document> afterDeleteIterator = index.searchAndQuery(Arrays.asList("import", "edu"));

        assertEquals(false, afterDeleteIterator.hasNext());
    }

    /**
     * Test deletion for searchOr operation
     */
    @Test
    public void testDeletionForSearchOr() {
        index.addDocument(documents[0]);
        index.flush();
        index.addDocument(documents[1]);
        index.flush();
        index.addDocument(documents[2]);
        index.flush();

        Iterator<Document> beforeDeleteIterator = index.searchOrQuery(Arrays.asList("import", "edu"));

        assertEquals(true, beforeDeleteIterator.hasNext());

        index.deleteDocuments("import");
        index.deleteDocuments("edu");

        Iterator<Document> afterDeleteIterator = index.searchOrQuery(Arrays.asList("import", "edu"));

        assertEquals(false, afterDeleteIterator.hasNext());
    }

    /**
     * Clean up the cache files
     */
    @After
    public void after() {
        File cacheFolder = new File(FOLDER);
        for (File file : cacheFolder.listFiles()) {
            try {
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cacheFolder.delete();
    }
}
