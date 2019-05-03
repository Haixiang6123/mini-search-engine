package edu.uci.ics.cs221.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
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
    // Base directory
    private Path basePath = null;
    // Segment num
    private int numSegments = 0;
    // Buffers
    private ByteBuffer wordsBuffer = null;
    private ByteBuffer listsBuffer = null;
    // Local document store
    private DocumentStore documentStore = null;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedLists = new HashMap<>();
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
     * Get Document Store instance
     */
    private DocumentStore getDocumentStore(int segumentNum) {
        return MapdbDocStore.createOrOpen(this.basePath.resolve("store" + segumentNum).toString());
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        if (this.invertedLists.size() >= DEFAULT_FLUSH_THRESHOLD) {
            this.flush();
        }
        // Add to in-memory documents map
        this.documentStore = this.getDocumentStore(this.numSegments);
        // Get new document ID
        int newDocId = (int) documentStore.size();
        // Add new document to store
        this.documentStore.addDocument(newDocId, document);

        // Use Analyzer to extract words from a document
        List<String> words = this.analyzer.analyze(document.getText());
        for (String word : words) {
            // Get documents that contain that word and store its ID
            List<Integer> documentIds = this.invertedLists.get(word);
            System.out.println(word + "---: " + Utils.stringifyList(documentIds));
            if (documentIds == null) {
                // Create a new list
                this.invertedLists.put(word, new ArrayList<>(Collections.singletonList(newDocId)));
            } else {
                // Add to exist list
                documentIds.add(newDocId);
            }
        }

        this.documentStore.close();
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
    private int flushWord(PageFileChannel wordsChannel, WordBlock wordBlock, int wordsPageNum) {
        int wordCapacity = Integer.BYTES + wordBlock.wordLength + Integer.BYTES + Integer.BYTES + Integer.BYTES;
        wordsPageNum = this.checkPageOutbound(wordsChannel, wordsBuffer, wordCapacity, wordsPageNum);

        // Word Block
        wordsBuffer
                .putInt(wordBlock.wordLength) // Word length
                .put(wordBlock.word.getBytes(StandardCharsets.US_ASCII)) // Word
                .putInt(wordBlock.listsPageNum) // Page num
                .putInt(wordBlock.listOffset) // Offset
                .putInt(wordBlock.listLength); // List length

        return wordsPageNum;
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        PageFileChannel listsChannel = this.getSegmentChannel(this.numSegments, "lists");
        PageFileChannel wordsChannel = this.getSegmentChannel(this.numSegments, "words");
        int wordsPageNum = 0;
        int listsPageNum = 0;

        for (String word : this.invertedLists.keySet()) {
            // Get document IDs by given word
            List<Integer> documentIds = this.invertedLists.get(word);

            // Mark down current lists buffer offset
            int listOffset = listsBuffer.position();

            // Check lists segment capacity
            listsPageNum = this.flushList(listsChannel, documentIds, listsPageNum);

            // Check words segment capacity
            WordBlock wordBlock = new WordBlock(
                    word.getBytes().length, // Word length
                    word,                   // Word
                    listsPageNum,           // Lists page num
                    listOffset,             // List offset
                    documentIds.size()      // List length
            );
            wordsPageNum = this.flushWord(wordsChannel, wordBlock, wordsPageNum);
        }

        // Write remaining content from buffer
        listsChannel.writePage(listsPageNum, listsBuffer);
        wordsChannel.writePage(wordsPageNum, wordsBuffer);
        listsChannel.close();
        wordsChannel.close();

        // Reset buffers
        wordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        listsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);

        // Increment segment number
        System.out.println("plus");
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

        Map<String, List<Integer>> invertedListsForTest = new HashMap<>();
        Map<Integer, Document> documentsForTest = new HashMap<>();

        while (wordsBuffer.position() < wordsBuffer.capacity()) {
            // Read word length
            int wordLength = wordsBuffer.getInt();
            if (wordLength == 0) { break; }

            WordBlock wordBlock = new WordBlock(
                    wordLength, // Word length
                    Utils.sliceStringFromBuffer(wordsBuffer, wordsBuffer.position(), wordLength), // Word
                    wordsBuffer.getInt(), // Lists page num
                    wordsBuffer.getInt(), // List offset
                    wordsBuffer.getInt()  // List length
            );

            // Update inverted lists
            List<Integer> invertedList = this.updateInvertedListsForTest(wordBlock, invertedListsForTest, listsFileChannel);

            // Update documents
            this.updateDocumentsForTest(segmentNum, invertedList, documentsForTest);
        }
        return new InvertedIndexSegmentForTest(invertedListsForTest, documentsForTest);
    }

    /**
     * Parse word block
     */
    private List<Integer> updateInvertedListsForTest(WordBlock wordBlock, Map<String, List<Integer>> invertedListsForTest, PageFileChannel listsFileChannel) {
        List<Integer> invertedList = new ArrayList<>();

        // Setup reading buffer
        listsBuffer = listsFileChannel.readPage(wordBlock.listsPageNum);
        listsBuffer.position(wordBlock.listOffset);

        // Read document IDs
        for (int i = 0; i < wordBlock.listLength; i++) {
            invertedList.add(listsBuffer.getInt());
        }

        invertedListsForTest.put(wordBlock.word, invertedList);

        return invertedList;
    }

    /**
     * Update documents
     */
    private void updateDocumentsForTest(int segmentNum, List<Integer> invertedListForTest, Map<Integer, Document> documentsForTest) {
        // Initialize document store
        DocumentStore documentStore = this.getDocumentStore(segmentNum);
        // Get current all document IDs
        Set<Integer> documentIds = documentsForTest.keySet();
        for (Integer documentId : invertedListForTest) {
            // If not exist, then store document in documents
            if (!documentIds.contains(documentId)) {
                documentsForTest.put(documentId, documentStore.getDocument(documentId));
            }
        }
        // Close document store
        documentStore.close();
    }
}
