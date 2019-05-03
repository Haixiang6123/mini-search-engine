package edu.uci.ics.cs221.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 * <p>
 * Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
     * <p>
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;

    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     * <p>
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    private int WORD_BLOCK = 20 + 8 + 8 + 8;

    // Native analyzer
    private Analyzer analyzer = null;
    // Native Page File Channel
    private PageFileChannel pageFileChannel = null;
    // In-memory data structure for storing inverted index
    private Map<String, List<Integer>> invertedLists = null;
    // Base directory
    private Path basePath = null;
    // Segment num
    private int numSegments = 0;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedLists = new HashMap<>();
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        List<String> tokens = this.analyzer.analyze(document.getText());
        for (String token : tokens) {
            List<Integer> documentIds = invertedLists.get(token);
            if (documentIds == null) {
                // Create a new list
                invertedLists.put(token, new ArrayList<>(Collections.singletonList(0)));
            } else {
                // Add to exist list
                documentIds.add(documentIds.size());
            }
        }
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        Set<String> words = this.invertedLists.keySet();

        int listsBufferLength = 0;
        for (String word: words) {
            listsBufferLength += this.invertedLists.get(word).size() * 4;
        }

        long listsPosition = 0;
        ByteBuffer wordsBuffer = ByteBuffer.allocate(words.size() * this.WORD_BLOCK);
        ByteBuffer listsBuffer = ByteBuffer.allocate(listsBufferLength);

        for (String word : words) {
            List<Integer> documentIds = this.invertedLists.get(word);
            // List Block
            for (Integer documentId : documentIds) {
                listsBuffer.putInt(documentId);
                System.out.println(documentId);
            }

            int listByteLength = documentIds.size() * 4;

            // Word Block
            wordsBuffer
                    .put(word.getBytes(StandardCharsets.US_ASCII))
                    .putLong(listsPosition / PageFileChannel.PAGE_SIZE)
                    .putLong(listsPosition - listsPosition / PageFileChannel.PAGE_SIZE)
                    .putLong(listByteLength);

            // Increment segment size
            listsPosition += listByteLength;
        }

        String s = new String(listsBuffer.array(), StandardCharsets.US_ASCII);
        String str = new String(wordsBuffer.array(), StandardCharsets.US_ASCII);
        System.out.println(s);
        System.out.println(str);

        // Write words buffer
        Path segmentWordsPath = basePath.resolve("segment" + this.numSegments + "_words");
        pageFileChannel = PageFileChannel.createOrOpen(segmentWordsPath);
        pageFileChannel.appendAllBytes(wordsBuffer);
        pageFileChannel.close();
        Path segmentListsPath = basePath.resolve("segment" + this.numSegments + "_lists");
        pageFileChannel = PageFileChannel.createOrOpen(segmentListsPath);
        pageFileChannel.appendAllBytes(listsBuffer);
        pageFileChannel.close();

        this.numSegments += 1;
    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        throw new UnsupportedOperationException();
    }

    /**
     * Performs a single keyword search on the inverted index.
     * You could assume the analyzer won't convert the keyword into multiple tokens.
     * If the keyword is empty, it should not return anything.
     *
     * @param keyword keyword, cannot be null.
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchQuery(String keyword) {
        Preconditions.checkNotNull(keyword);

        throw new UnsupportedOperationException();
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        throw new UnsupportedOperationException();
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        throw new UnsupportedOperationException();
    }

    /**
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     *
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the total number of segments in the inverted index.
     * This function is used for checking correctness in test cases.
     *
     * @return number of index segments.
     */
    public int getNumSegments() {
        return this.numSegments;
    }

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        Path segmentWordsPath = basePath.resolve("segment" + segmentNum + "_words");
        PageFileChannel wordsFileChannel = PageFileChannel.createOrOpen(segmentWordsPath);
        ByteBuffer wordsBuffer = wordsFileChannel.readAllPages();

        Path segmentListsPath = basePath.resolve("segment" + segmentNum + "_lists");
        PageFileChannel listsFileChannel = PageFileChannel.createOrOpen(segmentListsPath);
        ByteBuffer listsBuffer = listsFileChannel.readAllPages();

        Map<Integer, List<String>> invertedLists = new HashMap<>();
        Map<Integer, Document> documents = new HashMap<>();



        return null;
    }
}
