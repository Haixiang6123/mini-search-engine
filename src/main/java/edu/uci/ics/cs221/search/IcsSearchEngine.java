package edu.uci.ics.cs221.search;

import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;
import utils.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

public class IcsSearchEngine {
    private Path documentDirectory;
    private InvertedIndexManager indexManager;
    private Map<Integer, String> idUrl;
    private Map<Integer, List<Integer>> idGraph;

    /**
     * Initializes an IcsSearchEngine from the directory containing the documents and the
     *
     */
    public static IcsSearchEngine createSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        return new IcsSearchEngine(documentDirectory, indexManager);
    }

    private IcsSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        this.documentDirectory = documentDirectory;
        this.indexManager = indexManager;

        this.idUrl = new HashMap<>();
        this.idGraph = new HashMap<>();

        // Read url.tsv
        this.readUrlTsv(this.documentDirectory);

        // Read id-graph.tsv
        this.readIdGraphTsv(this.documentDirectory);
    }

    /**
     * Read url.tsv file
     * Store id-url pairs in hash map
     */
    private void readUrlTsv(Path documentDirectory) {
        File urlTsv = new File(documentDirectory.resolve("url.tsv").toString());
        FileUtils.readFileAsString(urlTsv, line -> {
            String[] idUrlStrings = line.split(" ");
            // Add to map
            idUrl.put(Integer.valueOf(idUrlStrings[0]), idUrlStrings[1]);
        });
    }

    /**
     * Read id-graph.tsv file
     * Store graph structure in hash map
     */
    private void readIdGraphTsv(Path documentDirectory) {
        File idGraphTsv = new File(documentDirectory.resolve("id-graph.tsv").toString());
        FileUtils.readFileAsString(idGraphTsv, line -> {
            String[] idPair = line.split(" ");
            int fromDocId = Integer.valueOf(idPair[0]);
            int toDocId = Integer.valueOf(idPair[1]);

            // Build up a graph
            if (idGraph.containsKey(fromDocId)) {
                idGraph.get(fromDocId).add(toDocId);
            }
            else {
                idGraph.put(fromDocId, new ArrayList<>(Collections.singletonList(toDocId)));
            }
        });
    }

    /**
     * Writes all ICS web page documents in the document directory to the inverted index.
     */
    public void writeIndex() {
        // Get document directory
        File documentDir = new File(this.documentDirectory.toString());
        // Get all document files
        File[] documents = documentDir.listFiles();
        if (documents == null) { return; }
        // Parse to documents
        for (File document : documents) {
            // Read document text
            String documentText = FileUtils.readFileAsString(document, null);

            // Add document to index manager
            indexManager.addDocument(new Document(documentText));
        }
    }

    /**
     * Computes the page rank score from the "id-graph.tsv" file in the document directory.
     * The results of the computation can be saved in a class variable and will be later retrieved by `getPageRankScores`.
     */
    public void computePageRank(int numIterations) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the page rank score of all documents previously computed. Must be called after `computePageRank`.
     * Returns an list of <DocumentID - Score> Pairs that is sorted by score in descending order (high scores first).
     */
    public List<Pair<Integer, Double>> getPageRankScores() {
        throw new UnsupportedOperationException();
    }

    /**
     * Searches the ICS document corpus and returns the top K documents ranked by combining TF-IDF and PageRank.
     *
     * The search process should first retrieve ALL the top documents from the InvertedIndex by TF-IDF rank,
     * by calling `searchTfIdf(query, null)`.
     *
     * Then the corresponding PageRank score of each document should be retrieved. (`computePageRank` will be called beforehand)
     * For each document, the combined score is  tfIdfScore + pageRankWeight * pageRankScore.
     *
     * Finally, the top K documents of the combined score are returned. Each element is a pair of <Document, combinedScore>
     *
     *
     * Note: We could get the Document ID by reading the first line of the document.
     * This is a workaround because our project doesn't support multiple fields. We cannot keep the documentID in a separate column.
     */
    public Iterator<Pair<Document, Double>> searchQuery(List<String> query, int topK, double pageRankWeight) {
        // Use TfIdf to search documents
        Iterator<Pair<Document, Double>> topKDocumentScores = this.indexManager.searchTfIdf(query, topK);
        // Retrieve corresponding PageRank score
        List<Pair<Integer, Double>> pageRankScores = this.getPageRankScores();
        // Combine scores
        while (topKDocumentScores.hasNext()) {
            Pair<Document, Double> documentScore = topKDocumentScores.next();

        }
        return topKDocumentScores;
    }

}
