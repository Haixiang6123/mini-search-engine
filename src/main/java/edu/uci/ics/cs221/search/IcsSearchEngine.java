package edu.uci.ics.cs221.search;

import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public class IcsSearchEngine {
    private Path documentDirectory;
    private InvertedIndexManager indexManager;

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
            try {
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(document), StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                // First line is document Id
                int docId = Integer.valueOf(bufferedReader.readLine());
                // Second line is the original URL
                String url = bufferedReader.readLine();
                // Third line is text of HTML document
                String text = bufferedReader.readLine();

                indexManager.addDocument(new Document(text));
            } catch (IOException e) {
                e.printStackTrace();
            }
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
     * Gets the page rank score of all documents previously computed. Must be called after `cmoputePageRank`.
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
        throw new UnsupportedOperationException();
    }

}
