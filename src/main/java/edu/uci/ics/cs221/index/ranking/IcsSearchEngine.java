package edu.uci.ics.cs221.index.ranking;

import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.storage.Document;
import javafx.util.Pair;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public class IcsSearchEngine {
    /**
     * Initializes an IcsSearchEngine from the directory containing the documents and the
     *
     */
    public static IcsSearchEngine createSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        return new IcsSearchEngine(documentDirectory, indexManager);
    }

    private IcsSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
    }

    /**
     * Writes all ICS web page documents in the document directory to the inverted index.
     */
    public void writeIndex() {
        throw new UnsupportedOperationException();
    }

    /**
     * Computes the page rank score from the id-graph file in the document directory.
     * The results of the computation can be saved in a class variable and will be later retrieved by `getPageRankScores`.
     */
    public void computePageRank() {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the page rank score of all documents previously computed. Must be called after `computePageRank`.
     * Returns a list of <DocumentID - Score> Pairs that is sorted by score in descending order (high scores first).
     */
    public List<Pair<Integer, Double>> getPageRankScores() {
        throw new UnsupportedOperationException();
    }

    /**
     * Searches the ICS document corpus with the query using TF-IDF search provided by inverted index manager.
     * The search process should first retrieve the top-K documents by TF-IDF rank,
     * then re-order the resulting documents by the page rank score.
     *
     * Note: you could get the ID of each document from its first line.
     */
    public Iterator<Document> searchQuery(List<String> query, int topK) {
        throw new UnsupportedOperationException();
    }
}
