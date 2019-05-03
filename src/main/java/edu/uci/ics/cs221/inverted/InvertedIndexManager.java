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
    private int listsBufferOffset = 0;
    // Local document store
    private DocumentStore documentStore = null;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedLists = new HashMap<>();
        this.listsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.wordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.wordsBuffer.putInt(0);
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
        // Add to in-memory documents map
        this.documentStore = this.getDocumentStore(this.numSegments);
        // Get new document ID
        int newDocId = (int) documentStore.size();
        // Add new document to store
        this.documentStore.addDocument(newDocId, document);
        this.documentStore.close();

        // Use Analyzer to extract words from a document
        List<String> words = this.analyzer.analyze(document.getText());
        for (String word : words) {
            // Get documents that contain that word and store its ID
            List<Integer> documentIds = this.invertedLists.get(word);
            if (documentIds == null) {
                // Create a new list
                this.invertedLists.put(word, new ArrayList<>(Collections.singletonList(newDocId)));
            } else {
                // Add to exist list
                documentIds.add(newDocId);
            }
        }

        // Auto flush
        if (newDocId + 1 >= DEFAULT_FLUSH_THRESHOLD) {
            this.flush();
        }
    }

    /**
     * Flush a list
     */
    private int flushList(PageFileChannel listsChannel, List<Integer> documentIds, int listsPageNum) {
        // Check lists segment capacity
        int listCapacity = documentIds.size() * Integer.BYTES;

        // Exceed capacity
        if (this.listsBuffer.position() + listCapacity >= this.listsBuffer.capacity()) {
            // Write to file
            listsChannel.writePage(listsPageNum, listsBuffer);
            // Increment total page num of lists
            listsPageNum += 1;
            // Allocate a new buffer
            this.listsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
            // Update lists buffer offset
            this.listsBufferOffset = 0;
        }

        // List Block
        for (Integer documentId : documentIds) {
            this.listsBuffer.putInt(documentId);
        }

        // Increment page num
        return listsPageNum;
    }

    /**
     * Flush a word
     */
    private int flushWord(PageFileChannel wordsChannel, WordBlock wordBlock, int wordsPageNum) {
        int wordBlockCapacity = wordBlock.getWordBlockCapacity();
        // Exceed capacity
        if (this.wordsBuffer.position() + wordBlockCapacity >= this.wordsBuffer.capacity()) {
            // Add total size of this page to the front
            this.wordsBuffer.putInt(0, this.wordsBuffer.position());
            // Write to file
            wordsChannel.writePage(wordsPageNum, this.wordsBuffer);
            // Increment total words page num
            wordsPageNum += 1;
            // Allocate a new buffer
            this.wordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
            // Initialize words page num
            this.wordsBuffer.putInt(0);
        }

        // Word Block
        this.wordsBuffer
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
        // Check if it is empty memory
        if (this.invertedLists.size() == 0) {
            return;
        }

        PageFileChannel listsChannel = this.getSegmentChannel(this.numSegments, "lists");
        PageFileChannel wordsChannel = this.getSegmentChannel(this.numSegments, "words");
        int wordsPageNum = 0;
        int listsPageNum = 0;

        for (String word : this.invertedLists.keySet()) {
            // Get document IDs by given word
            List<Integer> documentIds = this.invertedLists.get(word);

            // Mark down current lists buffer offset
            this.listsBufferOffset = listsBuffer.position();

            // Check lists segment capacity
            listsPageNum = this.flushList(listsChannel, documentIds, listsPageNum);

            // Check words segment capacity
            WordBlock wordBlock = new WordBlock(
                    word.getBytes().length, // Word length
                    word,                   // Word
                    listsPageNum,           // Lists page num
                    this.listsBufferOffset,             // List offset
                    documentIds.size()      // List length
            );
            wordsPageNum = this.flushWord(wordsChannel, wordBlock, wordsPageNum);
        }

        // Write remaining content from buffer
        this.wordsBuffer.putInt(0, this.wordsBuffer.position());
        wordsChannel.writePage(wordsPageNum, wordsBuffer);
        listsChannel.writePage(listsPageNum, listsBuffer);
        listsChannel.close();
        wordsChannel.close();

        // Reset buffers
        this.listsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.wordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        // Initialize words page num
        this.wordsBuffer.putInt(0);

        // Reset inverted lists
        this.invertedLists.clear();

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
        List<Document> documents = new ArrayList<>();
        // Append local segment documents in whole list
        for (int segmentNum = 0; segmentNum < this.numSegments; segmentNum++) {
            documents.addAll(this.getIndexSegment(segmentNum).getDocuments().values());
        }

        return documents.iterator();
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

        // Get total page num of words
        int wordsPagesNum = wordsFileChannel.getNumPages();

        Map<String, List<Integer>> invertedListsForTest = new HashMap<>();
        Map<Integer, Document> documentsForTest = new HashMap<>();

        for (int wordsPage = 0; wordsPage < wordsPagesNum; wordsPage++) {
            // Get byte buffer of this page
            ByteBuffer wordsPageBuffer = wordsFileChannel.readPage(wordsPage);
            // Get size of this page
            int wordsPageSize = wordsPageBuffer.getInt();
            while (wordsPageBuffer.position() < wordsPageSize) {
                // Read word length
                int wordLength = wordsPageBuffer.getInt();

                WordBlock wordBlock = new WordBlock(
                        wordLength, // Word length
                        Utils.sliceStringFromBuffer(wordsPageBuffer, wordsPageBuffer.position(), wordLength), // Word
                        wordsPageBuffer.getInt(), // Lists page num
                        wordsPageBuffer.getInt(), // List offset
                        wordsPageBuffer.getInt()  // List length
                );

                // Update inverted lists
                List<Integer> invertedList = this.updateInvertedListsForTest(wordBlock, invertedListsForTest, listsFileChannel);

                // Update documents
                this.updateDocumentsForTest(segmentNum, invertedList, documentsForTest);
            }
        }

        return invertedListsForTest.size() != 0 ?
                new InvertedIndexSegmentForTest(invertedListsForTest, documentsForTest) : null;
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
