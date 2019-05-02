package edu.uci.ics.cs221.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import org.eclipse.collections.api.set.primitive.CharSet;
import utils.Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
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

    // Native analyzer
    private Analyzer analyzer = null;
    // Native Page File Channel
    private PageFileChannel pageFileChannel = null;
    // In-memory data structure for storing inverted index
    private Map<String, List<Integer>> invertedIndexes = null;
    // Base directory
    private Path basePath = null;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedIndexes = new HashMap<>();
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
            List<Integer> documentIds = invertedIndexes.get(token);
            if (documentIds == null) {
                // Create a new list
                invertedIndexes.put(token, new ArrayList<>(Collections.singletonList(0)));
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
        Set<String> words = this.invertedIndexes.keySet();

        ByteBuffer wordsBuffer = ByteBuffer.allocate(words.size() * (20 + 8 + 8 + 8));
        ByteBuffer listsBuffer = ByteBuffer.allocate(0);
        long segmentSize = words.size() * (20 + 8 + 8 + 8);

        for (String word : words) {
            List<Integer> documentIds = this.invertedIndexes.get(word);
            // List Block
            ByteBuffer listBuffer = ByteBuffer.allocate(documentIds.size() * 4);
            for (Integer documentId : documentIds) {
                listBuffer.put(ByteBuffer.allocate(4).putInt(documentId));
            }

            // Append List block
            int listsBufferCapacity = listsBuffer.capacity() + listBuffer.capacity();
            listsBuffer = ByteBuffer.allocate(listsBufferCapacity)
                    .put(listsBuffer)
                    .put(listBuffer);

            // Word Block
            ByteBuffer wordBuffer = ByteBuffer.allocate(20).put(word.getBytes(StandardCharsets.UTF_8));
            ByteBuffer pageNumBuffer = ByteBuffer.allocate(8).putLong(segmentSize / PageFileChannel.PAGE_SIZE);
            ByteBuffer offsetBuffer = ByteBuffer.allocate(8).putLong(segmentSize - segmentSize / PageFileChannel.PAGE_SIZE);
            ByteBuffer lengthBuffer = ByteBuffer.allocate(8).putLong(listBuffer.capacity());

            // Append word block
            wordsBuffer = wordsBuffer
                    .put(wordBuffer)
                    .put(pageNumBuffer)
                    .put(offsetBuffer)
                    .put(lengthBuffer);

            // Increment segment size
            segmentSize += listBuffer.capacity();
        }

        // Write words buffer
        Path segmentWordsPath = basePath.resolve("segment" + PageFileChannel.writeCounter);
        pageFileChannel = PageFileChannel.createOrOpen(segmentWordsPath);
        pageFileChannel.appendAllBytes(wordsBuffer);
        pageFileChannel.appendAllBytes(listsBuffer);
        pageFileChannel.close();
//        System.out.println(wordsBuffer.toString());
//        String wordsStr = StandardCharsets.UTF_8.decode(wordsBuffer).toString();
//        String wordsStr = new String(wordsBuffer.ge,StandardCharsets.UTF_8);
//        System.out.println("length: " + wordsBuffer.getLong(48));
//        System.out.println("words: " + wordsStr);
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
        throw new UnsupportedOperationException();
    }

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        throw new UnsupportedOperationException();
    }
}
