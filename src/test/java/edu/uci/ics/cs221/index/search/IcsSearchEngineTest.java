package edu.uci.ics.cs221.index.search;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.NaiveAnalyzer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.index.positional.Compressor;
import edu.uci.ics.cs221.index.positional.DeltaVarLenCompressor;
import edu.uci.ics.cs221.search.IcsSearchEngine;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class IcsSearchEngineTest {
    private Compressor compressor = new DeltaVarLenCompressor();
    private Analyzer analyzer = new NaiveAnalyzer();
    private InvertedIndexManager indexManager;
    private IcsSearchEngine engine;
    private final String PATH = "./index/Team4IcsSearchEngineTest";

    @Before
    public void setup() {
        URL documentResource = IcsSearchEngine.class.getClassLoader().getResource("webpages");
        Path documentDirectory = Paths.get(documentResource.getPath());

        // Init engine
        indexManager = InvertedIndexManager.createOrOpenPositional(PATH, analyzer, compressor);
        engine = IcsSearchEngine.createSearchEngine(documentDirectory, indexManager);

        // Write all ICS documents to manager
        engine.writeIndex();

        // Compute page rank scores by given number of iterations
        engine.computePageRank(999);
    }

    @Test
    public void test1() {
        // Prepare search meta data
        List<String> queries = new ArrayList<>(Arrays.asList("Analysis", "Language", "for", "Distributed", "Embedded"));
        int topK = 10;
        double pageRankWeight = 0.15;

        // Start search
        Iterator<Pair<Document, Double>> iterator = engine.searchQuery(queries, topK, pageRankWeight);

        // Print out results
        this.printResults(iterator);
    }

    @Test
    public void test2() {
        // Prepare search meta data
        List<String> queries = new ArrayList<>(Arrays.asList("Downloads", "Publications", "DRE"));
        int topK = 10;
        double pageRankWeight = 0.15;

        // Start search
        Iterator<Pair<Document, Double>> iterator = engine.searchQuery(queries, topK, pageRankWeight);

        // Print out results
        this.printResults(iterator);
    }

    private void printResults(Iterator<Pair<Document, Double>> iterator) {
        // Print out all combined scores
        while (iterator.hasNext()) {
            Pair<Document, Double> pair = iterator.next();

            Document document = pair.getLeft();
            double combinedScore = pair.getRight();

            // Print out result
            System.out.println("Document: " + document.getText().substring(0, 10) + "...");
            System.out.println("Combined Score: " + combinedScore);
        }
    }

    @After
    public void teardown() {
        File cacheFolder = new File(PATH);
        for (File file : cacheFolder.listFiles()) {
            try {
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cacheFolder.delete();

        cacheFolder = new File(PATH);
        for (File file : cacheFolder.listFiles()) {
            try {
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cacheFolder.delete();
        new File(PATH).delete();
    }
}
