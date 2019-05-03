package edu.uci.ics.cs221.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import utils.Utils;

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
    // In-memory data structure for storing inverted index
    private Map<String, List<Integer>> invertedLists = null;
    // In-memory documents
    private Map<Integer, Document> documents = null;
    // Base directory
    private Path basePath = null;
    // Segment num
    private int numSegments = 0;
    // Buffers
    private ByteBuffer wordsBuffer = null;
    private ByteBuffer listsBuffer = null;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedLists = new HashMap<>();
        this.documents = new HashMap<>();
        this.wordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.listsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
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
     * Get segment channel by given keyword
     */
    private PageFileChannel getSegmentChannel(int segmentNum, String keyword) {
        return PageFileChannel.createOrOpen(basePath.resolve("segment" + segmentNum + "_" + keyword));
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        // Add to in-memory documents map
        int newDocId = documents.keySet().size();
        documents.put(newDocId, document);

        List<String> words = this.analyzer.analyze(document.getText());
        for (String word : words) {
            List<Integer> documentIds = invertedLists.get(word);
            if (documentIds == null) {
                // Create a new list
                invertedLists.put(word, new ArrayList<>(Collections.singletonList(newDocId)));
            } else {
                // Add to exist list
                documentIds.add(newDocId);
            }
        }
    }

    /**
     * Check for if current buffer exceed given capacity
     */
    private int checkPageOutbound(PageFileChannel fileChannel, ByteBuffer byteBuffer, int blockCapacity, int currentPageNum) {
        // Not exceed capacity
        if (byteBuffer.position() + blockCapacity <= listsBuffer.capacity()) {
            return currentPageNum;
        }

        // Write to file
        fileChannel.writePage(currentPageNum, listsBuffer);

        // Allocate a new buffer
        listsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        // Increment page num
        return currentPageNum + 1;
    }

    /**
     * Flush a list
     */
    private int flushList(PageFileChannel listsChannel, List<Integer> documentIds, int listsPageNum) {
        // Check lists segment capacity
        int listCapacity = documentIds.size() * Integer.BYTES;
        listsPageNum = this.checkPageOutbound(listsChannel, listsBuffer, listCapacity, listsPageNum);

        // List Block
        for (Integer documentId : documentIds) {
            listsBuffer.putInt(documentId);
        }

        return listsPageNum;
    }

    /**
     * Flush a word
     */
    private int flushWord(PageFileChannel wordsChannel, String word, int wordsPageNum, int listsPageNum, int listOffset, int listLength) {
        int wordCapacity = Integer.BYTES + word.getBytes().length + Integer.BYTES + Integer.BYTES + Integer.BYTES;
        wordsPageNum = this.checkPageOutbound(wordsChannel, wordsBuffer, wordCapacity, wordsPageNum);

        // Word Block
        wordsBuffer
                .putInt(word.getBytes().length) // Word length
                .put(word.getBytes(StandardCharsets.US_ASCII)) // Word
                .putInt(listsPageNum) // Page num
                .putInt(listOffset) // Offset
                .putInt(listLength); // List length

        return wordsPageNum;
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        System.out.println(Utils.stringifyHashMap(invertedLists));
        PageFileChannel listsChannel = this.getSegmentChannel(this.numSegments, "lists");
        PageFileChannel wordsChannel = this.getSegmentChannel(this.numSegments, "words");
        int wordsPageNum = 0;
        int listsPageNum = 0;

        // Mark down how many documents
        wordsBuffer.putInt(this.documents.size());

        for (String word : this.invertedLists.keySet()) {
            // Get document IDs by given word
            List<Integer> documentIds = this.invertedLists.get(word);

            // Mark down current lists buffer offset
            int listOffset = listsBuffer.position();

            // Check lists segment capacity
            listsPageNum = this.flushList(listsChannel, documentIds, listsPageNum);

            // Check words segment capacity
            wordsPageNum = this.flushWord(wordsChannel, word, wordsPageNum, listsPageNum, listOffset, documentIds.size());
        }

        // Write remaining content from buffer
        listsChannel.writePage(listsPageNum, listsBuffer);
        wordsChannel.writePage(wordsPageNum, wordsBuffer);
        listsChannel.close();
        wordsChannel.close();

        // Increment segment number
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
        PageFileChannel wordsFileChannel = this.getSegmentChannel(segmentNum, "words");
        PageFileChannel listsFileChannel = this.getSegmentChannel(segmentNum, "lists");

        ByteBuffer wordsBuffer = wordsFileChannel.readAllPages();
        wordsBuffer.flip();

        Map<String, List<Integer>> invertedLists = new HashMap<>();
        Map<Integer, Document> documents = new HashMap<>();

        int documentNum = wordsBuffer.getInt();

        while (wordsBuffer.position() < wordsBuffer.capacity()) {
            // Read word length
            int wordLength = wordsBuffer.getInt();
            if (wordLength == 0) { break; }
            // Read word
            String word = Utils.sliceStringFromBuffer(wordsBuffer, wordsBuffer.position(), wordLength);
            // Read page num
            int listPageNum = wordsBuffer.getInt();
            // Read length offset
            int listOffset = wordsBuffer.getInt();
            // Read list length
            int listLength = wordsBuffer.getInt();

            // Find list
            List<Integer> invertedList = new ArrayList<>();

            listsBuffer = listsFileChannel.readPage(listPageNum);
            listsBuffer.position(listOffset);
            for (int i = 0; i < listLength; i++) {
                int docID = listsBuffer.getInt();
                invertedList.add(docID);
            }
            invertedLists.put(word, invertedList);
        }
        System.out.println(Utils.stringifyHashMap(invertedLists));
        return null;
    }
}
