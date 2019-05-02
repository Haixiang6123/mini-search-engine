package edu.uci.ics.cs221.inverted;

import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Team18FlushTest {
    public InvertedIndexManager manager;
    String folderPath = "./index/Team18FlushTest";

    @Before
    public void initialize(){
        manager = InvertedIndexManager.createOrOpen(folderPath, new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer()));
    }


    Document doc1 = new Document("kitten, bunny");
    Document doc2 = new Document("bunny");
    PorterStemmer ps = new PorterStemmer();
    // test flush() when called and numSegments less than DEFAULT_FLUSH_THRESHOLD
    @Test
    public void test1(){
        manager.addDocument(doc1);
        manager.addDocument(doc2);
        manager.flush();
        int expectedNumSegments = 1;
        assertEquals(expectedNumSegments, manager.getNumSegments());

        InvertedIndexSegmentForTest iiTestSegment = manager.getIndexSegment(0);
        Map<String, List<Integer>> invertedLists = iiTestSegment.getInvertedLists();
        Map<Integer, Document> documents = iiTestSegment.getDocuments();
        assertEquals(2, invertedLists.size());
        assertEquals(2, documents.size());
        assertEquals(doc1, documents.get(0));
        assertEquals(doc2, documents.get(1));


        List<Integer> invertedList = invertedLists.get(ps.stem("kitten"));
        List<Integer> expectedInvertedList = Arrays.asList(0);
        assertEquals(expectedInvertedList, invertedList);
        invertedList = invertedLists.get(ps.stem("bunny"));
        expectedInvertedList = Arrays.asList(0, 1);
        Collections.sort(invertedList);
        assertEquals(expectedInvertedList, invertedList);

        

    }




    // test flush() has no operation when no document is added
    @Test
    public void test2(){
        manager.flush();
        assertEquals(null, manager.getIndexSegment(0));
    }




    // test flush() when DEFAULT_FLUSH_THRESHOLD documents is added
    @Test
    public void test3(){
        for(int i = 0; i < InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD; i ++)
            manager.addDocument(doc1);
        manager.addDocument(doc2);
        manager.flush();

        assertEquals(2, manager.getNumSegments());

        InvertedIndexSegmentForTest iiTestSegment = manager.getIndexSegment(0);
        Map<String, List<Integer>> invertedLists = iiTestSegment.getInvertedLists();
        Map<Integer, Document> documents = iiTestSegment.getDocuments();

        assertEquals(2, invertedLists.size());
        List<Integer> invertedList_1 = invertedLists.get(ps.stem("kitten"));
        List<Integer> invertedList_2 = invertedLists.get(ps.stem("bunny"));
        Collections.sort(invertedList_1);
        Collections.sort(invertedList_2);

        assertEquals(1000, documents.size());

        for(int i = 0; i < 1000; i ++){
            assertEquals(i, invertedList_1.get(i).intValue());
            assertEquals(i, invertedList_2.get(i).intValue());
            assertEquals(doc1, documents.get(i));
        }


    }

    @After
    public void clear(){
        File dir = new File("./index/Team18FlushTest");
        for (File file: dir.listFiles()){
            if (!file.isDirectory()){
                file.delete();
            }
        }
        dir.delete();
    }



}
