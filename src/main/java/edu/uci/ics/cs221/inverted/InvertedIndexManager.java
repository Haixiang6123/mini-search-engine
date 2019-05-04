package edu.uci.ics.cs221.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import utils.Utils;

import java.io.File;
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
    // Native analyzer
    private Analyzer analyzer = null;
    // In-memory data structure for storing inverted index
    private Map<String, List<Integer>> invertedLists = null;
    // Base directory
    private Path basePath = null;
    // Segment num
    private int numSegments = 0;
    // Local document store
    private DocumentStore documentStore = null;
    // Flush variables
    private ByteBuffer flushWordsBuffer = null;
    private ByteBuffer flushListsBuffer = null;
    // Merge variables
    private ByteBuffer mergeWordsBuffer = null;
    private ByteBuffer mergeListsBuffer = null;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedLists = new HashMap<>();
        // Flush variables init
        this.flushListsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.flushWordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.flushWordsBuffer.putInt(0);
        // Merge variables init
        this.mergeListsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.mergeWordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.mergeWordsBuffer.putInt(0);
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
    private DocumentStore getDocumentStore(int segmentNum, String suffix) {
        return MapdbDocStore.createOrOpen(this.basePath.resolve("store" + segmentNum + "_" + suffix).toString());
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        if (this.documentStore == null) {
            this.documentStore = this.getDocumentStore(this.numSegments, "");
        }
        // Get new document ID
        int newDocId = (int) this.documentStore.size();
        // Add new document to store
        this.documentStore.addDocument(newDocId, document);

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
    private void flushList(PageFileChannel listsChannel, ByteBuffer listsBuffer, List<Integer> documentIds, WriteMeta meta) {
        // Check lists segment capacity
        int listCapacity = documentIds.size() * Integer.BYTES;

        // Exceed capacity
        if (listsBuffer.position() + listCapacity >= listsBuffer.capacity()) {
            // Write to file
            listsChannel.writePage(meta.listsPageNum, flushListsBuffer);
            // Update meta
            meta.listsPageNum += 1;
            meta.listsPageOffset = 0;
            // Allocate a new buffer
            listsBuffer.clear();
        }

        // List Block
        for (Integer documentId : documentIds) {
            listsBuffer.putInt(documentId);
        }
    }

    /**
     * Flush a word
     */
    private void flushWord(PageFileChannel wordsChannel, ByteBuffer wordsBuffer, WordBlock wordBlock, WriteMeta meta) {
        int wordBlockCapacity = wordBlock.getWordBlockCapacity();
        // Exceed capacity
        if (wordsBuffer.position() + wordBlockCapacity >= wordsBuffer.capacity()) {
            // Add total size of this page to the front
            wordsBuffer.putInt(0, wordsBuffer.position());
            // Write to file
            wordsChannel.writePage(meta.wordsPageNum, wordsBuffer);
            // Increment total words page num
            meta.wordsPageNum += 1;
            // Allocate a new buffer
            wordsBuffer.clear();
            // Initialize words page num
            wordsBuffer.putInt(0);
        }

        // Word Block
        this.flushWordsBuffer
                .putInt(wordBlock.wordLength) // Word length
                .put(wordBlock.word.getBytes(StandardCharsets.US_ASCII)) // Word
                .putInt(wordBlock.listsPageNum) // Page num
                .putInt(wordBlock.listOffset) // Offset
                .putInt(wordBlock.listLength); // List length
    }

    private boolean isFlushValid() {
        if (this.documentStore == null) {
            return false;
        }
        long documentSize = this.documentStore.size();
        this.documentStore.close();
        return this.invertedLists.size() != 0 || documentSize != 0;
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        // Check if it is empty memory
        if (!this.isFlushValid()) {
            return;
        }

        // Reset document store
        if (this.documentStore != null) {
            this.documentStore.close();
            this.documentStore = null;
        }

        PageFileChannel listsChannel = this.getSegmentChannel(this.numSegments, "lists");
        PageFileChannel wordsChannel = this.getSegmentChannel(this.numSegments, "words");
        WriteMeta meta = new WriteMeta();

        for (String word : this.invertedLists.keySet()) {
            // Get document IDs by given word
            List<Integer> documentIds = this.invertedLists.get(word);

            // Mark down current lists buffer offset and current list page num
            meta.listsPageOffset = flushListsBuffer.position();

            // Check lists segment capacity
            this.flushList(listsChannel, this.flushListsBuffer, documentIds, meta);

            // Check words segment capacity
            WordBlock wordBlock = new WordBlock(
                    word.getBytes().length, // Word length
                    word,                   // Word
                    meta.listsPageNum,           // Lists page num
                    meta.listsPageOffset, // List offset
                    documentIds.size()      // List length
            );
            this.flushWord(wordsChannel, this.flushWordsBuffer, wordBlock, meta);
        }

        // Write remaining content from buffer
        this.flushWordsBuffer.putInt(0, this.flushWordsBuffer.position());
        wordsChannel.writePage(meta.wordsPageNum, flushWordsBuffer);
        listsChannel.writePage(meta.listsPageNum, flushListsBuffer);

        listsChannel.close();
        wordsChannel.close();

        // Reset Buffers
        this.resetBuffers();

        // Increment segment number
        this.numSegments += 1;
    }

    /**
     * Reset buffer before writing texts in segment
     */
    private void resetBuffers() {
        // Reset buffers
        this.flushListsBuffer.clear();
        this.flushWordsBuffer.clear();
        // Initialize words page num
        this.flushWordsBuffer.putInt(0);

        // Reset inverted lists
        this.invertedLists.clear();
    }

    /**
     * Need to rename segment before merge
     */
    private void renameBeforeMerge(int leftSegmentIndex, int rightSegmentIndex) {
        // Rename segments
        Utils.renameSegment(this.basePath, leftSegmentIndex, "lists", "lists_temp");
        Utils.renameSegment(this.basePath, leftSegmentIndex, "words", "words_temp");
        Utils.renameSegment(this.basePath, rightSegmentIndex, "lists", "lists_temp");
        Utils.renameSegment(this.basePath, rightSegmentIndex, "words", "words_temp");
    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
//        // merge only happens at even number of segments
//        Preconditions.checkArgument(getNumSegments() % 2 == 0);
//
//        // New segment page num
//        int wordsPageNum = 0;
//        int listsPageNum = 0;
//        int originListsPageNum = 0;
//        int listsPageOffset = 0;
//
//        // Merge all segments
//        for (int leftIndex = 0, rightIndex = 1; rightIndex < this.getNumSegments(); leftIndex++, rightIndex++) {
//            // Rename current 2 segments
//            this.renameBeforeMerge(leftIndex, rightIndex);
//            // New segment channels
//            PageFileChannel newSegWordsChannel = this.getSegmentChannel(leftIndex, "words");
//            PageFileChannel newSegListsChannel = this.getSegmentChannel(leftIndex, "lists");
//            // Original channels
//            PageFileChannel leftSegWordsChannel = this.getSegmentChannel(leftIndex, "words_temp");
//            PageFileChannel leftSegListsChannel = this.getSegmentChannel(leftIndex, "lists_temp");
//            PageFileChannel rightSegWordsChannel = this.getSegmentChannel(rightIndex, "words_temp");
//            PageFileChannel rightSegListsChannel = this.getSegmentChannel(rightIndex, "lists_temp");
//
//            // Get word blocks from left and right segment
//            List<WordBlock> leftWordBlocks = this.getWordBlocksFromSegment(leftSegWordsChannel, leftIndex);
//            List<WordBlock> rightWordBlocks = this.getWordBlocksFromSegment(rightSegWordsChannel, rightIndex);
//
//            // Merge word blocks
//            List<MergedWordBlock> mergedWordBlocks = Utils.mergeWordBlocks(leftWordBlocks, rightWordBlocks);
//
//            // Document store
//            this.mergeDocStores(leftIndex, rightIndex);
//
//            for (MergedWordBlock mergedWordBlock : mergedWordBlocks) {
//                if (mergedWordBlock.isSingle) {
//                    if (mergedWordBlock.leftWordBlock != null) {
//                        WordBlock leftWordBlock = mergedWordBlock.leftWordBlock;
//                        List<Integer> localInvertedList = this.getInvertedListFromSegment(
//                                leftSegListsChannel,
//                                leftWordBlock
//                        );
//
//                        // Mark down current lists buffer offset and current list page num
//                        listsPageOffset = flushListsBuffer.position();
//                        originListsPageNum = listsPageNum;
//
//                        // Write inverted list to segment
//                        listsPageNum = this.flushList(newSegListsChannel, this.mergeListsBuffer, localInvertedList, listsPageNum);
//                        listsPageOffset = originListsPageNum == listsPageNum ? listsPageOffset : 0;
//
//                        // Update word block
//                        leftWordBlock.listsPageNum = listsPageNum;
//                        leftWordBlock.listOffset = listsPageOffset;
//                        // Write word block to segment
//                        wordsPageNum = this.flushWord(newSegWordsChannel, this.mergeWordsBuffer, leftWordBlock, wordsPageNum);
//                    }
//                    else {
//                        WordBlock rightWordBlock = mergedWordBlock.rightWordBlock;
//                        List<Integer> localInvertedList = this.getInvertedListFromSegment(
//                                rightSegListsChannel,
//                                rightWordBlock
//                        );
//                        // Mark down current lists buffer offset and current list page num
//                        listsPageOffset = flushListsBuffer.position();
//                        originListsPageNum = listsPageNum;
//
//                        // Write inverted list to segment
//                        listsPageNum = this.flushList(newSegListsChannel, this.mergeListsBuffer, localInvertedList, listsPageNum);
//                        listsPageOffset = originListsPageNum == listsPageNum ? listsPageOffset : 0;
//
//                        // Update word block
//                        rightWordBlock.listsPageNum = listsPageNum;
//                        rightWordBlock.listOffset = listsPageOffset;
//                        // Write word block to segment
//                        wordsPageNum = this.flushWord(newSegWordsChannel, this.mergeWordsBuffer, rightWordBlock, wordsPageNum);
//                    }
//                }
//                else {
//
//                }
//            }
//        }
    }

    /**
     * Method to merge right document store to left document store
     */
    private void mergeDocStores(int leftIndex, int rightIndex) {
        // Rename document store file
        Utils.renameStore(this.basePath, leftIndex, "temp");
        DocumentStore leftDocStore = this.getDocumentStore(leftIndex, "temp");
        DocumentStore rightDocStore = this.getDocumentStore(rightIndex, "");
        DocumentStore newDocStore = this.getDocumentStore(leftIndex, "");
        // Get left doc store size
        int docSize = (int) leftDocStore.size();

        Iterator<Map.Entry<Integer, Document>> rightIterator = rightDocStore.iterator();
        Iterator<Map.Entry<Integer, Document>> leftIterator = rightDocStore.iterator();
        // Add left document store
        while (leftIterator.hasNext()) {
            Map.Entry<Integer, Document> entry = leftIterator.next();

            // Add origin left documents to new document store
            newDocStore.addDocument(entry.getKey(), entry.getValue());
        }
        // Add right document store
        while (rightIterator.hasNext()) {
            Map.Entry<Integer, Document> entry = rightIterator.next();

            // Update right documents ID and add it to left document store
            newDocStore.addDocument(docSize + entry.getKey(), entry.getValue());
        }
        // Close document stores
        leftDocStore.close();
        rightDocStore.close();
        newDocStore.close();
        // Delete temp files
        new File(this.basePath.resolve("store" + leftIndex + "_temp").toString()).delete();
        new File(this.basePath.resolve("store" + rightIndex).toString()).delete();
    }

    /**
     * Get all wordBlocks from a channel
     * @param wordsFileChannel
     * @return
     */
    private List<WordBlock> getWordBlocksFromSegment(PageFileChannel wordsFileChannel, int segmentIndex) {
        List<WordBlock> wordBlocks = new ArrayList<>();
        // Get page num
        int pagesNum = wordsFileChannel.getNumPages();
        // Iterate all pages
        for (int page = 0; page < pagesNum; page++) {
            // Get a byte buffer by given page
            ByteBuffer wordsBuffer = wordsFileChannel.readPage(page);
            // Get whole size
            int pageSize = wordsBuffer.getInt();
            while (wordsBuffer.position() < pageSize) {
                int wordLength = wordsBuffer.getInt();
                WordBlock wordBlock = new WordBlock(
                    wordLength, // Word length
                    Utils.sliceStringFromBuffer(wordsBuffer, wordsBuffer.position(), wordLength), // Word
                    wordsBuffer.getInt(), // Lists page num
                    wordsBuffer.getInt(),  // List offset
                    wordsBuffer.getInt()   // List length
                );
                wordBlock.segment = segmentIndex;
                wordBlocks.add(wordBlock);
            }
        }

        return wordBlocks;
    }

    /**
     * Get inverted list from segment
     */
    private List<Integer> getInvertedListFromSegment(PageFileChannel listsFileChannel, WordBlock wordBlock) {
        List<Integer> invertedList = new ArrayList<>();
        // Get byte buffer
        ByteBuffer listsByteBuffer = listsFileChannel.readPage(wordBlock.listsPageNum);
        // Move pointer to the offset
        listsByteBuffer.position(wordBlock.listOffset);
        // Extract the inverted list
        for (int i = 0; i < wordBlock.listLength; i++) {
            invertedList.add(listsByteBuffer.getInt());
        }
        return invertedList;
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
        ArrayList<Document> doc = new ArrayList<>();
        DocumentStore ds = MapdbDocStore.createOrOpen("docDB");
        int i = 0;
        while(true)   //traverse all segments
        {
            if(!Files.exists(basePath.resolve("segment" + i)))  //todo: is the number consecutive? or will have condition like(1,3,4)
            {
                break;
            }else{
                //read wordlist
                PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(basePath.resolve("segment" + i));
                ByteBuffer page = pageFileChannel.readPage(0);  // read metadata
                int numWordPages = page.getInt();   //number of pages that storing words
                //todo: read wordlists
                ArrayList<byte[]> words = new ArrayList<>();
                for(byte[] wdata : words)
                {
                    byte[] word = Arrays.copyOfRange(wdata,0, 20);   //get word
                    if (keyword.equals(new String( word, StandardCharsets.UTF_8 ))){
                        int offset = ByteBuffer.wrap(Arrays.copyOfRange(wdata,20,24)).getInt();
                        int length = ByteBuffer.wrap(Arrays.copyOfRange(wdata,24,28)).getInt();
                        //todo: read the wordlist
                        int[] postlist = new int[]{1,2};
                        for(Integer docId : postlist)
                        {
                            //todo: get knowledge of docFile path
                            doc.add(ds.getDocument(docId));
                        }
                    }
                }
            }
        }

        ds.close();
       return doc.iterator();

       /*   used for disk iterating
        Iterator<Document> it = new Iterator<Document>() {


            private int curSegment = 0;
            private int[] curPostList = null;
            private int curListIndex = 0;
            private String key = keyword;
            DocumentStore ds = MapdbDocStore.createOrOpen("docDB");

            @Override
            public boolean hasNext() {
                //     current index hasn't run out of boundary OR we have next segment to read
                if (curListIndex > curPostList.length||!Files.exists(basePath.resolve("segment" +i))
                        ds.close();  //close database
                return curListIndex < curPostList.length || Files.exists(basePath.resolve("segment" + i));
            }

            @Override
            public Document next() {
                //return arrayList[currentIndex++];
                if(curListIndex < curPostList.length)
                    return ds.getDocument(curPostList[curListIndex]);
                else{
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return it;

        */
        //throw new UnsupportedOperationException();
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        ArrayList<Document> doc = new ArrayList<>();
        DocumentStore ds = MapdbDocStore.createOrOpen("docDB");
        int i = 0;
        while(true)   //traverse all segments
        {
            if(!Files.exists(basePath.resolve("segment" + i)))  //todo: is the number consecutive? or will have condition like(1,3,4)
            {
                break;
            }else{
                //read wordlist
                PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(basePath.resolve("segment" + i));
                ByteBuffer page = pageFileChannel.readPage(0);  // read metadata
                int numWordPages = page.getInt();   //number of pages that storing words
                //todo: read wordlists
                ArrayList<byte[]> words = new ArrayList<>();
                for(byte[] wdata : words)
                {
                    byte[] word = Arrays.copyOfRange(wdata,0, 20);   //get word
                    if (keywords.get(0).equals(new String( word, StandardCharsets.UTF_8 ))){
                        int offset = ByteBuffer.wrap(Arrays.copyOfRange(wdata,20,24)).getInt();
                        int length = ByteBuffer.wrap(Arrays.copyOfRange(wdata,24,28)).getInt();
                        //todo: read the wordlist
                        int[] postlist = new int[]{1,2};
                        for(Integer docId : postlist)
                        {
                            //todo: get knowledge of docFile path
                            doc.add(ds.getDocument(docId));
                        }
                    }
                }
            }
        }

        ds.close();
        return doc.iterator();


        //throw new UnsupportedOperationException();
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        List<Document> result = new ArrayList<>();
        while(true)        //todo: condition -- search for existing segments
        {
            //1. read word list of segment

            //2. get length of each word's wordlist

            //3. loop: for each word, get the wordlist from disk, then OR the list (idx) with current list.

            //4. retrieve the documents to List<Document>
            break;
        }

        //todo: how to return iterator that could access disk upon request ?
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
        PageFileChannel listsFileChannel = this.getSegmentChannel(segmentNum, "lists");
        PageFileChannel wordsFileChannel = this.getSegmentChannel(segmentNum, "words");

        Map<String, List<Integer>> invertedListsForTest = this.getInvertedListsForTest(listsFileChannel, wordsFileChannel, segmentNum);
        Map<Integer, Document> documentsForTest = this.getDocumentsForTest(segmentNum);

        return documentsForTest.size() != 0 ?
                new InvertedIndexSegmentForTest(invertedListsForTest, documentsForTest) : null;
    }

    /**
     * Get inverted lists from segment
     */
    private Map<String, List<Integer>> getInvertedListsForTest(PageFileChannel listsFileChannel, PageFileChannel wordsFileChannel, int segmentNum) {
        Map<String, List<Integer>> invertedListsForTest = new HashMap<>();

        List<WordBlock> wordBlocks = this.getWordBlocksFromSegment(wordsFileChannel, segmentNum);

        for (WordBlock wordBlock : wordBlocks) {
            List<Integer> invertedList = this.getInvertedListFromSegment(listsFileChannel, wordBlock);
            invertedListsForTest.put(wordBlock.word, invertedList);
        }

        return invertedListsForTest;
    }

    /**
     * Get documents from segment
     */
    private Map<Integer, Document> getDocumentsForTest(int segmentNum) {
        Map<Integer, Document> documentsForTest = new HashMap<>();

        DocumentStore documentStore = this.getDocumentStore(segmentNum, "");
        long documentSize = documentStore.size();
        for (int id = 0; id < documentSize; id++) {
            documentsForTest.put(id, documentStore.getDocument(id));
        }

        documentStore.close();
        return documentsForTest;
    }
}
