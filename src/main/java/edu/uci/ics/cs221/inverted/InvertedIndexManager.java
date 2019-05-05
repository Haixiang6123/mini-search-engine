package edu.uci.ics.cs221.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import org.apache.lucene.util.ArrayUtil;
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
    //in memory documents
    private Map<Integer,Document> inMemDocs = null;

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
            this.documentStore = this.getDocumentStore(this.numSegments, ""); //todo shall we create it upon flush?
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
        wordsBuffer
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

            // Check words segment capacity
            WordBlock wordBlock = new WordBlock(
                    word.getBytes().length, // Word length
                    word,                   // Word
                    meta.listsPageNum,           // Lists page num
                    meta.listsPageOffset, // List offset
                    documentIds.size()      // List length
            );

            // Flush word and list
            this.flushWordAndList(
                    listsChannel, wordsChannel,
                    this.flushListsBuffer, this.flushWordsBuffer,
                    documentIds, wordBlock,
                    meta);
        }

        // Write remaining content from buffer
        this.flushWordsBuffer.putInt(0, this.flushWordsBuffer.position());
        wordsChannel.writePage(meta.wordsPageNum, this.flushWordsBuffer);
        listsChannel.writePage(meta.listsPageNum, this.flushListsBuffer);

        listsChannel.close();
        wordsChannel.close();

        // Reset Buffers
        this.resetFlushBuffers();
        // Reset inverted lists
        this.invertedLists.clear();

        // Increment segment number
        this.numSegments += 1;

        // Check if it needs to merge
        if (this.getNumSegments() >= DEFAULT_MERGE_THRESHOLD) {
            this.mergeAllSegments();
        }
    }

    /**
     * Reset buffers before writing texts in segment
     */
    private void resetFlushBuffers() {
        // Reset buffers
        this.flushListsBuffer.clear();
        this.flushWordsBuffer.clear();
        // Initialize words page num
        this.flushWordsBuffer.putInt(0);
    }

    /**
     * Reset buffers before merge
     */
    private void resetMergeBuffers() {
        // Reset buffers
        this.mergeListsBuffer.clear();
        this.mergeWordsBuffer.clear();
        // Initialize words page num
        this.mergeWordsBuffer.putInt(0);
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
        // New segment page num
        WriteMeta meta = new WriteMeta();

        // Merge all segments
        for (int leftIndex = 0, rightIndex = 1; rightIndex < this.getNumSegments(); leftIndex++, rightIndex++) {
            // Rename current 2 segments
            this.renameBeforeMerge(leftIndex, rightIndex);
            // New segment channels
            PageFileChannel newSegWordsChannel = this.getSegmentChannel(leftIndex, "words");
            PageFileChannel newSegListsChannel = this.getSegmentChannel(leftIndex, "lists");
            // Original channels
            PageFileChannel leftSegWordsChannel = this.getSegmentChannel(leftIndex, "words_temp");
            PageFileChannel leftSegListsChannel = this.getSegmentChannel(leftIndex, "lists_temp");
            PageFileChannel rightSegWordsChannel = this.getSegmentChannel(rightIndex, "words_temp");
            PageFileChannel rightSegListsChannel = this.getSegmentChannel(rightIndex, "lists_temp");

            // Get word blocks from left and right segment
            List<WordBlock> leftWordBlocks = this.getWordBlocksFromSegment(leftSegWordsChannel, leftIndex);
            List<WordBlock> rightWordBlocks = this.getWordBlocksFromSegment(rightSegWordsChannel, rightIndex);

            // Merge word blocks
            List<MergedWordBlock> mergedWordBlocks = Utils.mergeWordBlocks(leftWordBlocks, rightWordBlocks);

            // Document store
            int baseDocSize = this.mergeDocStores(leftIndex, rightIndex);

            for (MergedWordBlock mergedWordBlock : mergedWordBlocks) {
                WordBlock leftWordBlock = mergedWordBlock.leftWordBlock;
                WordBlock rightWordBlock = mergedWordBlock.rightWordBlock;
                List<Integer> leftInvertedList = this.getInvertedListFromSegment(
                        leftSegListsChannel,
                        leftWordBlock
                );
                List<Integer> rightInvertedList = this.getInvertedListFromSegment(
                        rightSegListsChannel,
                        rightWordBlock
                );
                Utils.increaseDocId(baseDocSize, rightInvertedList);
                if (mergedWordBlock.isSingle) {
                    if (leftWordBlock != null) {
                        this.flushWordAndList(
                                newSegListsChannel, newSegWordsChannel,
                                this.mergeListsBuffer, this.mergeWordsBuffer,
                                leftInvertedList, leftWordBlock,
                                meta);
                    }
                    else {
                        this.flushWordAndList(
                                newSegListsChannel, newSegWordsChannel,
                                this.mergeListsBuffer, this.mergeWordsBuffer,
                                rightInvertedList, rightWordBlock,
                                meta);
                    }
                }
                else {
                    List<Integer> localInvertedList = new ArrayList<>();
                    localInvertedList.addAll(leftInvertedList);
                    localInvertedList.addAll(rightInvertedList);
                    // Update list length in word block
                    leftWordBlock.listLength += rightWordBlock.listLength;
                    this.flushWordAndList(
                            newSegListsChannel, newSegWordsChannel,
                            this.mergeListsBuffer, this.mergeWordsBuffer,
                            localInvertedList, leftWordBlock,
                            meta);
                }
            }

            // Write remaining content from buffer
            this.mergeWordsBuffer.putInt(0, this.mergeWordsBuffer.position());
            newSegWordsChannel.writePage(meta.wordsPageNum, this.mergeWordsBuffer);
            newSegListsChannel.writePage(meta.listsPageNum, this.mergeListsBuffer);

            newSegListsChannel.close();
            newSegListsChannel.close();

            // Reset buffers
            this.resetMergeBuffers();
        }

        this.numSegments = this.numSegments / 2;

        // Delete temp files
        Utils.deleteTempFiles(this.basePath);

        // Merge if segment num is still larger than threshold
        if (this.getNumSegments() >= DEFAULT_MERGE_THRESHOLD) {
            this.mergeAllSegments();
        }
    }

    /**
     * Flush word block and list
     */
    private void flushWordAndList(PageFileChannel listsChannel, PageFileChannel wordsChannel,
                                  ByteBuffer listsBuffer, ByteBuffer wordsBuffer,
                                  List<Integer> invertedList, WordBlock wordBlock, WriteMeta meta) {
        // Mark down current lists buffer offset and current list page num
        meta.listsPageOffset = listsBuffer.position();

        // Write inverted list to segment
        this.flushList(listsChannel, listsBuffer, invertedList, meta);

        // Update word block
        wordBlock.listsPageNum = meta.listsPageNum;
        wordBlock.listOffset = meta.listsPageOffset;
        // Write word block to segment
        this.flushWord(wordsChannel, wordsBuffer, wordBlock, meta);
    }

    /**
     * Method to merge right document store to left document store
     */
    private int mergeDocStores(int leftIndex, int rightIndex) {
        // Rename document store file
        Utils.renameStore(this.basePath, leftIndex, "temp");
        DocumentStore leftDocStore = this.getDocumentStore(leftIndex, "temp");
        DocumentStore rightDocStore = this.getDocumentStore(rightIndex, "");
        DocumentStore newDocStore = this.getDocumentStore(leftIndex, "");
        // Get left doc store size
        int baseDocSize = (int) leftDocStore.size();

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
            newDocStore.addDocument(baseDocSize + entry.getKey(), entry.getValue());
        }
        // Close document stores
        leftDocStore.close();
        rightDocStore.close();
        newDocStore.close();
        // Delete temp files
        new File(this.basePath.resolve("store" + leftIndex + "_temp").toString()).delete();
        new File(this.basePath.resolve("store" + rightIndex).toString()).delete();

        return baseDocSize;
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
        if (wordBlock == null) {
            return invertedList;
        }
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
        //1. stemming the keyword
        List<String> keywords = this.analyzer.analyze(keyword);
        if( keywords == null || keywords.size() == 0 || keywords.get(0).equals(""))
            return doc.iterator();

        keyword = keywords.get(0);

        int i = 0;
        while(true)   //traverse all segments
        {
            if(!Files.exists(basePath.resolve("segment" + i +"_words"))) { // the numbers are consecutive thus could stop when i cannot be accessed
                break;
            }else{

                DocumentStore ds = getDocumentStore(i,"");

                //read wordlists  todo: stop upon the target word found
                PageFileChannel wordsChannel = getSegmentChannel(i,"words");
                int numWordPages = wordsChannel.getNumPages();   //number of pages of words
                ArrayList<WordBlock> words = new ArrayList<>();

                for (int p = 0; p < numWordPages; p++)
                {
                    ByteBuffer page = wordsChannel.readPage(p);
                    int end = page.getInt();

                    while(page.position() < end){
                        //read word block
                        int wordLength = page.getInt();
                        WordBlock wb = new WordBlock(
                                wordLength,
                                Utils.sliceStringFromBuffer(page, page.position(), wordLength),
                                page.getInt(),
                                page.getInt(),
                                page.getInt());
                        words.add(wb);
                    }
                }

                wordsChannel.close();

                PageFileChannel listChannel = getSegmentChannel(i, "lists");
                for(WordBlock wdata : words)   //search the word sequentially
                {
                    if (keyword.equals(wdata.word)){
                        //read posting list from file
                        ByteBuffer listPage = listChannel.readPage(wdata.listsPageNum);
                        //store docID
                        int[] postList = new int[wdata.listLength];
                        listPage.position(wdata.listOffset);
                        for(int l = 0; l < wdata.listLength; l++)
                            postList[l] = listPage.getInt();

                        for(int docId : postList)
                        {
                            doc.add(ds.getDocument(docId));
                            System.out.println("segment : " + i + "docId:" + docId + ds.getDocument(docId).getText());
                        }
                        break;   //break if the word is found
                    }
                }
                listChannel.close();
                ds.close();
            }
            i++;
        }

        return doc.iterator();  //todo iterator for docStore?

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
        //analyze key words
        ArrayList<String> analyzed = new ArrayList<>();
        for( String k : keywords) {
            List<String> ana = this.analyzer.analyze(k);
            if (ana != null && ana.size() > 0 && !ana.get(0).equals(""))  // not empty string
                analyzed.add(ana.get(0));
            else                      // and "" -> no result
                return doc.iterator();
        }


        int i = 0;
        while(true)   //traverse all segments
        {
            if(!Files.exists(basePath.resolve("segment" + i +"_words")))
            {
                break;
            }else{
                //open docDB for this segment
                DocumentStore ds = this.getDocumentStore(i, "");

                //read wordlist: only read those in anaylized lists
                PageFileChannel wordsChannel = this.getSegmentChannel(i, "words");
                int numWordPages = wordsChannel.getNumPages();   //number of pages of words
                ArrayList<WordBlock> words = new ArrayList<>();

                for (int p = 0; p < numWordPages; p++)
                {
                    ByteBuffer page = wordsChannel.readPage(p);
                    int end = page.getInt();

                    while(page.position() < end){
                        //read word block
                        int wordLength = page.getInt();
                        String word = Utils.sliceStringFromBuffer(page, page.position(), wordLength);
                        if(analyzed.contains(word)) {    //ArrayList.contains() tests equals(), not object identity
                            WordBlock wb = new WordBlock(
                                    wordLength,
                                    word,
                                    page.getInt(),
                                    page.getInt(),
                                    page.getInt());
                            //add the word in search query
                            words.add(wb);
                        }
                    }
                }

                wordsChannel.close();

                if(words.size() != analyzed.size())   // And query exists some words not in this segment; todo better checking method
                    continue;

                //retrive the lists and merge with basic
                ArrayList<Integer> intersection = null;
                //sort the words' list ; merge the list from short list to longer list
                words.sort(new Comparator<WordBlock>() {
                               @Override
                               public int compare(WordBlock o1, WordBlock o2) {
                                   return o1.listLength - o2.listLength;
                               }
                           });

                PageFileChannel listChannel = getSegmentChannel(i, "lists");
                for(WordBlock wdata : words)
                {
                    //read posting list from file
                    ByteBuffer listPage = listChannel.readPage(wdata.listsPageNum);
                    //store docID
                    Integer[] postList = new Integer[wdata.listLength];
                    listPage.position(wdata.listOffset);
                    for(int l = 0; l < wdata.listLength; l++)
                        postList[l] = listPage.getInt();

                    if(intersection == null) {
                        intersection = new ArrayList<>(Arrays.asList(postList));
                    }else{
                        //find intersection:  by binary search
                        ArrayList<Integer> temp = new ArrayList<>();
                        int lowbound = 0;   //lowerbound for list being searched; the ids are sorted in posting list
                        for(Integer target : intersection){
                            int l = lowbound, r = wdata.listLength - 1;
                            while (l < r){
                                int mid = (l + r)/2;
                                if( postList[mid].compareTo(target) < 0 )   //Integer comparation
                                    l = mid + 1;
                                else    //postList[mid] >= target
                                    r = mid;
                            }
                            if(postList[r].compareTo(target) == 0)    //equals: add the number to new arraylist
                            {
                                temp.add(target);
                                lowbound = r + 1;   //raise the search range's lower bound
                            }
                        }
                    }
                }
                listChannel.close();
                //read doc
                for(int docId : intersection)
                {
                    doc.add(ds.getDocument(docId));
                }
                ds.close();
            }
            i++;
        }

        return doc.iterator();
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        List<Document> doc = new ArrayList<>();
        //analyze key words
        ArrayList<String> analyzed = new ArrayList<>();
        for( String k : keywords) {
            List<String> ana = this.analyzer.analyze(k);
            if (ana != null && ana.size() > 0 && !ana.get(0).equals(""))  // not empty string  //todo can't filter ""
                analyzed.add(ana.get(0));
            else                      // and "" -> no result
                return doc.iterator();
        }


        int i = 0;
        while(true)
        {
            if(!Files.exists(basePath.resolve("segment" + i +"_words")))
            {
                break;
            }else{
                //open docDB for this segment
                DocumentStore ds = this.getDocumentStore(i, "");

                //1. read word list of segment
                PageFileChannel wordsChannel = this.getSegmentChannel(i, "words");
                int numWordPages = wordsChannel.getNumPages();   //number of pages of words
                ArrayList<WordBlock> words = new ArrayList<>();

                for (int p = 0; p < numWordPages; p++)
                {
                    ByteBuffer page = wordsChannel.readPage(p);
                    int end = page.getInt();

                    while(page.position() < end){
                        //read word block
                        int wordLength = page.getInt();
                        String word = Utils.sliceStringFromBuffer(page, page.position(), wordLength);
                        if(analyzed.contains(word)) {    //ArrayList.contains() tests equals(), not object identity
                            WordBlock wb = new WordBlock(
                                    wordLength,
                                    word,
                                    page.getInt(),
                                    page.getInt(),
                                    page.getInt());
                            //add the word in search query
                            words.add(wb);
                        }
                    }
                }

                wordsChannel.close();

                //retrive the lists and merge with basic
                TreeSet<Integer> union = new TreeSet<>();
                PageFileChannel listChannel = this.getSegmentChannel(i, "lists");
                for(WordBlock wdata : words)   //search the word sequentially
                {
                    //read posting list from file
                    ByteBuffer listPage = listChannel.readPage(wdata.listsPageNum);
                    //store docID
                    Integer[] postList = new Integer[wdata.listLength];
                    listPage.position(wdata.listOffset);
                    for(int l = 0; l < wdata.listLength; l++)
                        postList[l] = listPage.getInt();

                    //use set to do union todo: optimiaze the way of doing OR
                    union.addAll(Arrays.asList(postList));

                }
                listChannel.close();
                //retrieve the documents to List<Document>
                for(int docId : union)
                {
                    doc.add(ds.getDocument(docId));
                }
                ds.close();
            }
            i++;
        }
        return doc.iterator();
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

        listsFileChannel.close();
        wordsFileChannel.close();

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
