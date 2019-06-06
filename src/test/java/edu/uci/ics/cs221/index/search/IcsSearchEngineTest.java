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
    private final String PATH = "./index/Team4IcsSearchEngineTest";
    private Path documentDirectory = null;

    @Before
    public void setup() {
        // TODO: Check webpages path
        URL documentResource = IcsSearchEngine.class.getClassLoader().getResource("webpages");
        documentDirectory = Paths.get(documentResource.getPath());
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

    @Test
    public void test1() {
        InvertedIndexManager indexManager = InvertedIndexManager.createOrOpenPositional(PATH, analyzer, compressor);
        IcsSearchEngine engine = IcsSearchEngine.createSearchEngine(documentDirectory, indexManager);

        // Write all ICS documents to manager
        engine.writeIndex();

        // Compute page rank scores by given number of iterations
        engine.computePageRank(999);

        // Search queries
        List<String> queries = new ArrayList<>(Arrays.asList("Analysis", "Language", "for", "Distributed", "Embedded"));
        int topK = 10;
        double pageRankWeight = 0.15;
        Iterator<Pair<Document, Double>> iterator = engine.searchQuery(queries, topK, pageRankWeight);

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

}
