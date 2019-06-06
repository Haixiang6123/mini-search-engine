package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.index.positional.Compressor;
import edu.uci.ics.cs221.index.positional.DeltaVarLenCompressor;
import edu.uci.ics.cs221.index.positional.NaiveCompressor;
import edu.uci.ics.cs221.index.positional.PositionalIndexSegmentForTest;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import edu.uci.ics.cs221.index.inverted.Pair;
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
import java.util.stream.Collectors;

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
    private ByteBuffer flushPosBuffer = null;
    // Merge variables
    private ByteBuffer mergeWordsBuffer = null;
    private ByteBuffer mergeListsBuffer = null;
    private ByteBuffer mergePosBuffer = null;
    // In memory documents
    private Map<Integer, Document> documents = null;
    // Deleted documents
    private List<String> deletedWords = null;
    // Compressor
    private Compressor compressor = null;
    // Support
    private boolean supportPosition = false;
    private Compressor naiveCompressor = new NaiveCompressor();


    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedLists = new HashMap<>();
        this.documents = new HashMap<>();
        this.deletedWords = new ArrayList<>();
        // Flush variables init
        this.flushListsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.flushWordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.flushPosBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.flushWordsBuffer.putInt(0);
        // Merge variables init
        this.mergeListsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.mergeWordsBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        this.mergePosBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
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
                    InvertedIndexManager manager = new InvertedIndexManager(indexFolder, analyzer);
                    manager.supportPosition = false;
                    // make sure manager can run if not positional supported.
                    manager.compressor = new DeltaVarLenCompressor();
                    return manager;
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                InvertedIndexManager manager = new InvertedIndexManager(indexFolder, analyzer);
                manager.supportPosition = false;
                manager.compressor = new DeltaVarLenCompressor();
                return manager;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a positional index with the given folder, analyzer, and the compressor.
     * Compressor must be used to compress the inverted lists and the position lists.
     */
    public static InvertedIndexManager createOrOpenPositional(String indexFolder, Analyzer analyzer, Compressor compressor) {
        InvertedIndexManager manager = createOrOpen(indexFolder, analyzer);
        manager.supportPosition = true;
        manager.compressor = compressor;

        return manager;
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
        // Get new document ID
        int newDocId = this.documents.size();
        // Add new document to store
        this.documents.put(newDocId, document);

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
                if (!documentIds.contains(newDocId))
                    documentIds.add(newDocId);
            }
        }

        // Auto flush
        if (newDocId + 1 >= DEFAULT_FLUSH_THRESHOLD) {
            this.flush();
        }
    }

    private boolean isFlushValid() {
        return this.invertedLists.size() != 0 || this.documents.size() != 0;
    }

    /**
     * Flush documents from memory to disk
     */
    private void flushDocuments() {
        // Add documents from memory to disk
        this.documentStore = this.getDocumentStore(this.numSegments, "");
        for (int id = 0; id < this.documents.size(); id++) {
            this.documentStore.addDocument(id, this.documents.get(id));
        }
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

        this.flushDocuments();

        PageFileChannel listsChannel = this.getSegmentChannel(this.numSegments, "lists");
        PageFileChannel wordsChannel = this.getSegmentChannel(this.numSegments, "words");
        PageFileChannel posChannel = this.getSegmentChannel(this.numSegments, "positions");
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
                    documentIds.size(),      // List length
                    meta.listsPageNum * PageFileChannel.PAGE_SIZE + this.flushListsBuffer.position(),
                    0
            );

            // Flush word and list
            this.flushWordAndList(
                    this.documentStore,
                    listsChannel, wordsChannel, posChannel,
                    this.flushListsBuffer, this.flushWordsBuffer, this.flushPosBuffer,
                    documentIds, wordBlock,
                    meta);
        }

        // Write remaining content from buffer
        this.flushWordsBuffer.putInt(0, this.flushWordsBuffer.position());
        wordsChannel.writePage(meta.wordsPageNum, this.flushWordsBuffer);
        listsChannel.writePage(meta.listsPageNum, this.flushListsBuffer);
        posChannel.writePage(meta.posPageNum, this.flushPosBuffer);

        listsChannel.close();
        wordsChannel.close();
        posChannel.close();

        // Reset Buffers
        this.resetFlushBuffers();
        // Reset inverted lists
        this.invertedLists.clear();
        this.documents.clear();

        // Close document store
        this.documentStore.close();
        this.documentStore = null;

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
        this.flushPosBuffer.clear();
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
        this.mergePosBuffer.clear();
        // Initialize words page num
        this.mergeWordsBuffer.putInt(0);
    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // New segment page num
        WriteMeta meta = new WriteMeta();

        // Merge all segments
        for (int leftIndex = 0, rightIndex = 1; rightIndex < this.getNumSegments(); leftIndex += 2, rightIndex += 2) {
            int newIndex = leftIndex / 2;
            // New segment channels
            PageFileChannel newSegWordsChannel = this.getSegmentChannel(newIndex, "words_new");
            PageFileChannel newSegListsChannel = this.getSegmentChannel(newIndex, "lists_new");
            PageFileChannel newSegPosChannel = this.getSegmentChannel(newIndex, "positions_new");
            // Original channels
            PageFileChannel leftSegWordsChannel = this.getSegmentChannel(leftIndex, "words");
            PageFileChannel leftSegListsChannel = this.getSegmentChannel(leftIndex, "lists");
            PageFileChannel leftSegPosChannel = this.getSegmentChannel(leftIndex, "positions");
            PageFileChannel rightSegWordsChannel = this.getSegmentChannel(rightIndex, "words");
            PageFileChannel rightSegListsChannel = this.getSegmentChannel(rightIndex, "lists");
            PageFileChannel rightSegPosChannel = this.getSegmentChannel(rightIndex, "positions");
            // Document store
            DocumentStore leftDocStore = this.getDocumentStore(leftIndex, "");
            DocumentStore rightDocStore = this.getDocumentStore(rightIndex, "");
            DocumentStore newDocStore = this.getDocumentStore(newIndex, "new");

            // Get word blocks from left and right segment
            List<WordBlock> leftWordBlocks = this.getWordBlocksFromSegment(leftSegWordsChannel, leftIndex);
            List<WordBlock> rightWordBlocks = this.getWordBlocksFromSegment(rightSegWordsChannel, rightIndex);

            // Merge word blocks
            List<MergedWordBlock> mergedWordBlocks = Utils.mergeWordBlocks(leftWordBlocks, rightWordBlocks, this.deletedWords);

            // Document store
            int baseDocSize = this.mergeDocStores(leftDocStore, rightDocStore, newDocStore);

            for (MergedWordBlock mergedWordBlock : mergedWordBlocks) {
                WordBlock leftWordBlock = mergedWordBlock.leftWordBlock;
                WordBlock rightWordBlock = mergedWordBlock.rightWordBlock;
                ListBlock leftListBlock = this.getListBlockFromSegment(
                        leftSegListsChannel,
                        leftWordBlock
                );
                ListBlock rightListBlock = this.getListBlockFromSegment(
                        rightSegListsChannel,
                        rightWordBlock
                );
                Utils.increaseDocId(baseDocSize, rightListBlock.invertedList);

                // Compute merge flag
                int flag = this.computeMergeFlag(mergedWordBlock, leftWordBlock);

                // Start to merge
                this.mergeWordAndList(newSegListsChannel, newSegWordsChannel, newSegPosChannel,
                        leftSegPosChannel, rightSegPosChannel,
                        this.mergeListsBuffer, this.mergeWordsBuffer, this.mergePosBuffer,
                        leftListBlock, rightListBlock,
                        flag == 1 ? rightWordBlock : leftWordBlock, meta, flag);
            }

            // Write remaining content from buffer
            this.mergeWordsBuffer.putInt(0, this.mergeWordsBuffer.position());
            newSegWordsChannel.writePage(meta.wordsPageNum, this.mergeWordsBuffer);
            newSegListsChannel.writePage(meta.listsPageNum, this.mergeListsBuffer);
            newSegPosChannel.writePage(meta.posPageNum, this.mergePosBuffer);

            // Close channels
            newSegListsChannel.close();
            newSegWordsChannel.close();
            newSegPosChannel.close();
            leftSegListsChannel.close();
            leftSegWordsChannel.close();
            leftSegPosChannel.close();
            rightSegListsChannel.close();
            rightSegWordsChannel.close();
            rightSegPosChannel.close();
            // Close document stores
            leftDocStore.close();
            rightDocStore.close();
            newDocStore.close();

            // Delete origin files
            (new File(this.basePath.resolve("segment" + leftIndex + "_words").toString())).delete();
            (new File(this.basePath.resolve("segment" + leftIndex + "_lists").toString())).delete();
            (new File(this.basePath.resolve("segment" + leftIndex + "_positions").toString())).delete();
            (new File(this.basePath.resolve("segment" + rightIndex + "_words").toString())).delete();
            (new File(this.basePath.resolve("segment" + rightIndex + "_lists").toString())).delete();
            (new File(this.basePath.resolve("segment" + rightIndex + "_positions").toString())).delete();
            // Delete origin document store files
            (new File(this.basePath.resolve("store" + leftIndex + "_").toString())).delete();
            (new File(this.basePath.resolve("store" + rightIndex + "_").toString())).delete();
            // Rename current 2 segments
            Utils.renameSegment(this.basePath, newIndex, "words_new", "words");
            Utils.renameSegment(this.basePath, newIndex, "lists_new", "lists");
            Utils.renameSegment(this.basePath, newIndex, "positions_new", "positions");
            Utils.renameStore(this.basePath, newIndex, "new", "");

            // Reset buffers
            this.resetMergeBuffers();
        }

        this.deletedWords.clear();

        this.numSegments = this.numSegments / 2;
    }

    /**
     * Compute merge flag
     * 0: only left side
     * 1: only right side
     * 2: both sides
     */
    private int computeMergeFlag(MergedWordBlock mergedWordBlock, WordBlock leftWordBlock) {
        if (mergedWordBlock.isSingle) {
            if (leftWordBlock != null) {
                return 0;
            } else {
                return 1;
            }
        } else {
            return 2;
        }
    }

    /**
     * Merge: word block and list block
     */
    private void mergeWordAndList(PageFileChannel listsChannel, PageFileChannel wordsChannel, PageFileChannel posChannel,
                                  PageFileChannel leftPosChannel, PageFileChannel rightPosChannel,
                                  ByteBuffer listsBuffer, ByteBuffer wordsBuffer, ByteBuffer posBuffer,
                                  ListBlock leftListBlock, ListBlock rightListBlock,
                                  WordBlock wordBlock, WriteMeta meta, int flag) {
        // Update word block
        wordBlock.listsPageNum = meta.listsPageNum;
        wordBlock.listOffset = listsBuffer.position();

        // Global offsets
        // Start to extract position lists and merge them
        List<Integer> globalOffsets = this.mergePositionList(posChannel, posBuffer,
                leftPosChannel, rightPosChannel,
                leftListBlock, rightListBlock,
                meta, flag);

        // Encode invertedList
        List<Integer> invertedList = null;
        List<Integer> sizeList = null;
        switch (flag) {
            // Only left side
            case 0:
                invertedList = leftListBlock.invertedList;
                sizeList = leftListBlock.sizeList;
                break;
            case 1:
                invertedList = rightListBlock.invertedList;
                sizeList = rightListBlock.sizeList;
                break;
            case 2:
                invertedList = new ArrayList<>();
                invertedList.addAll(leftListBlock.invertedList);
                invertedList.addAll(rightListBlock.invertedList);
                sizeList = new ArrayList<>();
                sizeList.addAll(leftListBlock.sizeList);
                sizeList.addAll(rightListBlock.sizeList);
                break;
        }
        byte[] encodedInvertedList = this.compressor.encode(invertedList);
        // Encode global offset
        byte[] encodedGlobalOffsets = this.compressor.encode(globalOffsets);
        // Encode size list
        byte[] encodedSizeList = this.naiveCompressor.encode(sizeList);
        // Update size length
        wordBlock.sizeLength = encodedSizeList.length;

        // Markdown global offset bytes length
        wordBlock.listLength = encodedInvertedList.length;
        wordBlock.globalOffsetLength = encodedGlobalOffsets.length;

        // Flush list block
        this.flushListBlock(listsChannel, listsBuffer, encodedInvertedList, encodedGlobalOffsets, encodedSizeList, meta);

        // Flush word block
        this.flushWordBlock(wordsChannel, wordsBuffer, wordBlock, meta);
    }

    /**
     * Merge: flush position list
     */
    private List<Integer> mergePositionList(PageFileChannel posChannel, ByteBuffer posBuffer,
                                            PageFileChannel leftPosChannel, PageFileChannel rightPosChannel,
                                            ListBlock leftListBlock, ListBlock rightListBlock,
                                            WriteMeta meta, int flag) {
        // Merged global offsets
        List<Integer> globalOffsets = new ArrayList<>();
        if (!this.supportPosition) {
            return globalOffsets;
        }

        switch (flag) {
            // Only left side
            case 0:
                // Compute global offsets on left side
                this.assembleGlobalOffsets(leftPosChannel, posChannel, posBuffer, leftListBlock, globalOffsets, meta);
                break;
            // Only right side
            case 1:
                // Compute global offsets on right side
                this.assembleGlobalOffsets(rightPosChannel, posChannel, posBuffer, rightListBlock, globalOffsets, meta);
                break;
            // Both sides
            case 2:
                // Compute global offsets on left side
                this.assembleGlobalOffsets(leftPosChannel, posChannel, posBuffer, leftListBlock, globalOffsets, meta);
                // Remove the last one
                globalOffsets.remove(globalOffsets.size() - 1);
                // Compute global offsets on right side
                this.assembleGlobalOffsets(rightPosChannel, posChannel, posBuffer, rightListBlock, globalOffsets, meta);
                break;
        }

        return globalOffsets;
    }

    /**
     * Assemble global offsets on left side or right side
     */
    private void assembleGlobalOffsets(PageFileChannel readPosChannel, PageFileChannel writePosChannel,
                                       ByteBuffer posBuffer, ListBlock listBlock, List<Integer> globalOffsets,
                                       WriteMeta meta) {
        for (int i = 0; i < listBlock.invertedList.size(); i++) {
            // Get position list
            List<Integer> positionList = this.getPositionList(readPosChannel, listBlock.globalOffsets, i);
            // Encode position list
            byte[] encodedPositionList = this.compressor.encode(positionList);
            // Mark down global offset
            globalOffsets.add(meta.posPageNum * PageFileChannel.PAGE_SIZE + posBuffer.position());

            // Flush encoded position list
            for (byte encodedPosition : encodedPositionList) {
                if (posBuffer.position() >= posBuffer.capacity()) {
                    writePosChannel.writePage(meta.posPageNum, posBuffer);
                    meta.posPageNum += 1;
                    posBuffer.clear();
                }
                posBuffer.put(encodedPosition);
            }
        }
        // Add end offset
        globalOffsets.add(meta.posPageNum * PageFileChannel.PAGE_SIZE + posBuffer.position());
    }

    /**
     * Flush word block and list
     */
    private void flushWordAndList(DocumentStore documentStore,
                                  PageFileChannel listsChannel, PageFileChannel wordsChannel, PageFileChannel posChannel,
                                  ByteBuffer listsBuffer, ByteBuffer wordsBuffer, ByteBuffer posBuffer,
                                  List<Integer> invertedList, WordBlock wordBlock, WriteMeta meta) {
        // Update word block
        wordBlock.listsPageNum = meta.listsPageNum;
        wordBlock.listOffset = listsBuffer.position();

        // Global offsets
        List<Integer> globalOffsets = new ArrayList<>();
        // Position list sizes list
        byte[] encodedSizeList = new byte[0];
        // Deal with position list
        if (this.supportPosition) {
            // Get size list
            List<Integer> sizeList = this.flushPositionList(documentStore, posChannel, posBuffer, invertedList, globalOffsets, wordBlock, meta);
            // Encode size list
            encodedSizeList = this.naiveCompressor.encode(sizeList);
        }
        // Update word block size length
        wordBlock.sizeLength = encodedSizeList.length;

        // Encode invertedList
        byte[] encodedInvertedList = this.compressor.encode(invertedList);
        // Encode global offset
        byte[] encodedGlobalOffsets = this.compressor.encode(globalOffsets);

        // Markdown global offset bytes length
        wordBlock.listLength = encodedInvertedList.length;
        wordBlock.globalOffsetLength = encodedGlobalOffsets.length;

        // Flush list block
        this.flushListBlock(listsChannel, listsBuffer, encodedInvertedList, encodedGlobalOffsets, encodedSizeList, meta);

        // Flush word block
        this.flushWordBlock(wordsChannel, wordsBuffer, wordBlock, meta);
    }

    /**
     * Init inverted list and position list
     */
    private List<Integer> flushPositionList(DocumentStore documentStore, PageFileChannel posChannel, ByteBuffer posBuffer,
                                            List<Integer> invertedList, List<Integer> globalOffsets,
                                            WordBlock wordBlock, WriteMeta meta) {
        List<Integer> sizeList = new ArrayList<>();
        // Flush all position lists
        for (Integer id : invertedList) {
            // Get position list
            List<Integer> positionList = Utils.getPositions(documentStore.getDocument(id), wordBlock.word, this.analyzer);
            // Add size
            sizeList.add(positionList.size());
            // Encode position list
            byte[] encodedPositionList = this.compressor.encode(positionList);
            // Mark down global offset
            globalOffsets.add(meta.posPageNum * PageFileChannel.PAGE_SIZE + posBuffer.position());

            // Flush encoded position list
            for (byte encodedPosition : encodedPositionList) {
                if (posBuffer.position() >= posBuffer.capacity()) {
                    posChannel.writePage(meta.posPageNum, posBuffer);
                    meta.posPageNum += 1;
                    posBuffer.clear();
                }
                posBuffer.put(encodedPosition);
            }
        }
        // Add end offset
        globalOffsets.add(meta.posPageNum * PageFileChannel.PAGE_SIZE + posBuffer.position());

        return sizeList;
    }

    /**
     * Flush inverted list and global offsets
     */
    private void flushListBlock(PageFileChannel listsChannel, ByteBuffer listsBuffer, byte[] encodedInvertedList, byte[] encodedGlobalOffsets, byte[] encodedSizeList, WriteMeta meta) {
        // Flush inverted list
        for (byte encodedDocId : encodedInvertedList) {
            if (listsBuffer.position() >= listsBuffer.capacity()) {
                listsChannel.writePage(meta.listsPageNum, listsBuffer);
                meta.listsPageNum += 1;
                listsBuffer.clear();
            }
            listsBuffer.put(encodedDocId);
        }
        // Flush global offsets
        for (byte encodedGlobalOffset : encodedGlobalOffsets) {
            if (listsBuffer.position() >= listsBuffer.capacity()) {
                listsChannel.writePage(meta.listsPageNum, listsBuffer);
                meta.listsPageNum += 1;
                listsBuffer.clear();
            }
            listsBuffer.put(encodedGlobalOffset);
        }
        // Flush size list
        for (byte encodedSize : encodedSizeList) {
            if (listsBuffer.position() >= listsBuffer.capacity()) {
                listsChannel.writePage(meta.listsPageNum, listsBuffer);
                meta.listsPageNum += 1;
                listsBuffer.clear();
            }
            listsBuffer.put(encodedSize);
        }
    }

    /**
     * Flush word block to file
     */
    private void flushWordBlock(PageFileChannel wordsChannel, ByteBuffer wordsBuffer, WordBlock wordBlock, WriteMeta meta) {
        // Write word block to segment
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
        wordsBuffer.putInt(wordBlock.wordLength) // Word length
                .put(wordBlock.word.getBytes(StandardCharsets.UTF_8)) // Word
                .putInt(wordBlock.listsPageNum) // Page num
                .putInt(wordBlock.listOffset) // Offset
                .putInt(wordBlock.listLength) // List length
                .putInt(wordBlock.globalOffsetLength) // Global offset length
                .putInt(wordBlock.sizeLength); // Size length
    }

    /**
     * Method to merge right document store to left document store
     */
    private int mergeDocStores(DocumentStore leftDocStore, DocumentStore rightDocStore, DocumentStore newDocStore) {
        // Rename document store file
        // Get left doc store size
        int baseDocSize = (int) leftDocStore.size();

        Iterator<Map.Entry<Integer, Document>> leftIterator = leftDocStore.iterator();
        Iterator<Map.Entry<Integer, Document>> rightIterator = rightDocStore.iterator();
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

        return baseDocSize;
    }

    /**
     * Search word block by given word block list and word list
     */
    private List<WordBlock> filterWordBlock(List<WordBlock> wordBlocks, List<String> wordList) {
        List<WordBlock> results = new ArrayList<>();
        for (WordBlock wordBlock : wordBlocks) {
            if (wordList.contains(wordBlock.word)) { //&& !results.contains(wordBlock)
                results.add(wordBlock);
            }
        }

        return results;
    }

    /**
     * Get all wordBlocks from a channel
     *
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
                        wordsBuffer.getInt(),   // List length
                        wordsBuffer.getInt(),   //  Global offset length
                        wordsBuffer.getInt()   // Size list length
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
    private ListBlock getListBlockFromSegment(PageFileChannel listsFileChannel, WordBlock wordBlock) {
        if (wordBlock == null) {
            return new ListBlock(0, 0, 0);
        }
        // Init a new list block
        ListBlock listBlock = new ListBlock(wordBlock.listLength, wordBlock.globalOffsetLength, wordBlock.sizeLength);

        // Get byte buffer
        int page = wordBlock.listsPageNum;
        ByteBuffer listsByteBuffer = listsFileChannel.readPage(page);
        // Move pointer to the offset
        listsByteBuffer.position(wordBlock.listOffset);

        // Extract the inverted list
        for (int i = 0; i < wordBlock.listLength; i++) {
            // Overflow -> span out pages
            if (listsByteBuffer.position() >= listsByteBuffer.capacity()) {
                page += 1;
                listsByteBuffer = listsFileChannel.readPage(page);
            }
            listBlock.encodedInvertedList[i] = listsByteBuffer.get();
        }
        // Decode inverted list
        listBlock.invertedList = this.compressor.decode(listBlock.encodedInvertedList);

        // Extract the global offset
        for (int i = 0; i < wordBlock.globalOffsetLength; i++) {
            // Overflow -> span out pages
            if (listsByteBuffer.position() >= listsByteBuffer.capacity()) {
                page += 1;
                listsByteBuffer = listsFileChannel.readPage(page);
            }
            listBlock.encodedGlobalOffsets[i] = listsByteBuffer.get();
        }
        // Decode global offsets
        listBlock.globalOffsets = this.compressor.decode(listBlock.encodedGlobalOffsets);

        // Extract the size list
        for (int i = 0; i < wordBlock.sizeLength; i++) {
            // Overflow -> span out pages
            if (listsByteBuffer.position() >= listsByteBuffer.capacity()) {
                page += 1;
                listsByteBuffer = listsFileChannel.readPage(page);
            }
            listBlock.encodedSizeList[i] = listsByteBuffer.get();
        }
        // Decode size list
        listBlock.sizeList = this.naiveCompressor.decode(listBlock.encodedSizeList);

        return listBlock;
    }

    /**
     * Filter deleted keywords inverted list when searching
     */
    private List<WordBlock> filterDeletedWordBlocks(List<WordBlock> wordBlocks) {
        List<WordBlock> filteredWordsBlocks = new ArrayList<>();
        for (WordBlock wordBlock : wordBlocks) {
            if (!this.deletedWords.contains(wordBlock.word)) {
                filteredWordsBlocks.add(wordBlock);
            }
        }
        return filteredWordsBlocks;
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
        if (keywords == null || keywords.size() == 0 || keywords.get(0).equals(""))
            return doc.iterator();

        keyword = keywords.get(0);

        int i = 0;
        //traverse all segments
        while (true) {
            // the numbers are consecutive thus could stop when i cannot be accessed
            if (!Files.exists(basePath.resolve("segment" + i + "_words"))) {
                break;
            } else {
                DocumentStore documentStore = getDocumentStore(i, "");

                // Read word list
                PageFileChannel wordsChannel = getSegmentChannel(i, "words");
                PageFileChannel listChannel = getSegmentChannel(i, "lists");

                // Get all word blocks
                List<WordBlock> words = this.getWordBlocksFromSegment(wordsChannel, i);
                // Filter word blocks
                List<WordBlock> filteredWords = this.filterWordBlock(words, Arrays.asList(keyword));
                filteredWords = this.filterDeletedWordBlocks(filteredWords);

                for (WordBlock wordBlock : filteredWords) {
                    ListBlock listBlock = this.getListBlockFromSegment(listChannel, wordBlock);

                    for (int docId : listBlock.invertedList) {
                        doc.add(documentStore.getDocument(docId));
                    }
                }

                listChannel.close();
                wordsChannel.close();
                documentStore.close();
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
        for (String keyword : keywords) {
            List<String> result = this.analyzer.analyze(keyword);
            if (result != null && result.size() > 0 && !result.get(0).equals(""))
                analyzed.addAll(result);
            else
                return doc.iterator();
        }

        int i = 0;
        //traverse all segments
        while (true) {
            if (!Files.exists(basePath.resolve("segment" + i + "_words"))) {
                break;
            } else {
                //open docDB for this segment
                DocumentStore documentStore = this.getDocumentStore(i, "");

                // Read word list: only read those in analyzed lists
                PageFileChannel wordsChannel = this.getSegmentChannel(i, "words");

                // Get all word blocks
                List<WordBlock> wordBlocks = this.getWordBlocksFromSegment(wordsChannel, i);

                // Filter word blocks
                List<WordBlock> filteredWordBlocks = this.filterWordBlock(wordBlocks, analyzed);
                filteredWordBlocks = this.filterDeletedWordBlocks(filteredWordBlocks);

                // And query exists some words not in this segment
                if (filteredWordBlocks.size() != analyzed.size()) {
                    documentStore.close();
                    wordsChannel.close();
                    i++;
                    continue;
                }

                // Retrieve the lists and merge with basic
                ArrayList<Integer> intersection = null;
                // Sort the words' list ; merge the list from short list to longer list
                filteredWordBlocks.sort(Comparator.comparingInt(o -> o.listLength));

                PageFileChannel listChannel = getSegmentChannel(i, "lists");
                for (WordBlock wordBlock : filteredWordBlocks) {
                    // Get inverted list
                    ListBlock listBlock = this.getListBlockFromSegment(listChannel, wordBlock);

                    if (intersection == null) {
                        intersection = new ArrayList<>(listBlock.invertedList);
                    } else {
                        // Find intersection: by binary search
                        ArrayList<Integer> result = new ArrayList<>();
                        // Lowerbound for list being searched; the ids are sorted in posting list
                        int lowbound = 0;
                        for (Integer target : intersection) {
                            int left = lowbound, right = listBlock.invertedList.size() - 1;
                            while (left < right) {
                                int mid = (left + right) / 2;
                                //Integer comparision
                                if (listBlock.invertedList.get(mid).compareTo(target) < 0)
                                    left = mid + 1;
                                else    //postList[mid] >= target
                                    right = mid;
                            }
                            // Equals: add the number to new ArrayList
                            if (listBlock.invertedList.get(right).compareTo(target) == 0) {
                                result.add(target);
                                lowbound = right + 1;   //raise the search range's lower bound
                            }
                        }
                        // Update intersection
                        intersection = result;
                    }
                }
                //read doc
                for (int docId : intersection) {
                    doc.add(documentStore.getDocument(docId));
                }
                documentStore.close();
                wordsChannel.close();
                listChannel.close();
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
        // Analyze key words
        List<String> analyzed = new ArrayList<>();
        for (String keyword : keywords) {
            analyzed.addAll(this.analyzer.analyze(keyword));
        }

        // Filter empty string
        analyzed = analyzed.stream()
                .filter((String keyword) -> !keyword.equals(""))
                .collect(Collectors.toList());

        // If analyze is empty, return
        if (analyzed.size() == 0) {
            return doc.iterator();
        }

        int i = 0;
        while (true) {
            if (!Files.exists(basePath.resolve("segment" + i + "_words"))) {
                break;
            } else {
                //open docDB for this segment
                DocumentStore documentStore = this.getDocumentStore(i, "");

                //1. read word list of segment
                PageFileChannel wordsChannel = this.getSegmentChannel(i, "words");

                // Get word blocks
                List<WordBlock> wordBlocks = this.getWordBlocksFromSegment(wordsChannel, i);

                // Filter word blocks
                List<WordBlock> filteredWordBlocks = this.filterWordBlock(wordBlocks, analyzed);
                filteredWordBlocks = this.filterDeletedWordBlocks(filteredWordBlocks);

                // Retrieve the lists and merge with basic
                TreeSet<Integer> union = new TreeSet<>();
                PageFileChannel listChannel = this.getSegmentChannel(i, "lists");
                for (WordBlock wordBlock : filteredWordBlocks) {
                    // Get inverted list
                    ListBlock listBlock = this.getListBlockFromSegment(listChannel, wordBlock);

                    // Use set to do union
                    union.addAll(listBlock.invertedList);
                }

                // Retrieve the documents to List<Document>
                for (int docId : union) {
                    doc.add(documentStore.getDocument(docId));
                }

                // Close channels
                listChannel.close();
                wordsChannel.close();
                documentStore.close();
            }
            i++;
        }
        return doc.iterator();
    }

    /**
     * Performs a phrase search on a positional index.
     * Phrase search means the document must contain the consecutive sequence of keywords in exact order.
     * <p>
     * You could assume the analyzer won't convert each keyword into multiple tokens.
     * TODO: Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param phrase, a consecutive sequence of keywords
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchPhraseQuery(List<String> phrase) {
        if (this.supportPosition == false)
            throw new UnsupportedOperationException();

        Preconditions.checkNotNull(phrase);

        List<Document> documents = new ArrayList<>();

        // Analyze phrase words
        ArrayList<String> analyzed = new ArrayList<>();
        for (String keyword : phrase) {
            List<String> result = this.analyzer.analyze(keyword);
            if (result != null && result.size() > 0 && !result.get(0).equals(""))  // This word might be a stop word
                analyzed.addAll(result);
        }

        // Return null if no words left.
        if (analyzed.size() == 0)
            return documents.iterator();

        int i = 0;
        // Traverse all segments
        while (true) {
            if (!Files.exists(basePath.resolve("segment" + i + "_words"))) {
                break;
            } else {
                //open docDB for this segment
                DocumentStore documentStore = this.getDocumentStore(i, "");

                // Read word list: only read those in analyzed lists
                PageFileChannel wordsChannel = this.getSegmentChannel(i, "words");

                // Get all word blocks
                List<WordBlock> wordBlocks = this.getWordBlocksFromSegment(wordsChannel, i);

                // Filter word blocks
                List<WordBlock> filteredWordBlocks = this.filterWordBlock(wordBlocks, analyzed);
                filteredWordBlocks = this.filterDeletedWordBlocks(filteredWordBlocks);

                // Jump to next round if Phrase exists some words not in this segment
                if (filteredWordBlocks.size() != analyzed.size()) {
                    documentStore.close();
                    wordsChannel.close();
                    i++;
                    continue;
                }

                // 2. Use And Method to get a docID list
                // Retrieve the lists and merge with basic
                ArrayList<Integer> intersection = null;

                // Sort the words' list ; merge the list from short list to longer list
                filteredWordBlocks.sort(Comparator.comparingInt(o -> o.listLength));
                PageFileChannel listChannel = getSegmentChannel(i, "lists");
                for (WordBlock wordBlock : filteredWordBlocks) {
                    // Get inverted list
                    ListBlock listBlock = this.getListBlockFromSegment(listChannel, wordBlock);
                    List<Integer> invertedList = listBlock.invertedList;

                    if (intersection == null) {
                        intersection = new ArrayList<>(invertedList);
                    } else {
                        // Find intersection: by binary search
                        ArrayList<Integer> result = new ArrayList<>();
                        // Lowerbound for list being searched; the ids are sorted in posting list
                        int lowbound = 0;
                        for (Integer target : intersection) {
                            int left = lowbound, right = invertedList.size() - 1;
                            while (left < right) {
                                int mid = (left + right) / 2;
                                //Integer comparision
                                if (invertedList.get(mid).compareTo(target) < 0)
                                    left = mid + 1;
                                else    //postList[mid] >= target
                                    right = mid;
                            }
                            // Equals: add the number to new ArrayList
                            if (invertedList.get(right).compareTo(target) == 0) {
                                result.add(target);
                                lowbound = right + 1;   //raise the search range's lower bound
                            }
                        }
                        // Update intersection
                        intersection = result;
                    }
                }

                // 3. Validate processed docID
                if (intersection == null) {
                    documentStore.close();
                    wordsChannel.close();
                    listChannel.close();
                    return documents.iterator();
                }
                // Organize words into Hashmap. ensure the sequence of words in phrase
                Map<String, WordBlock> filteredWordBlocksMap = new HashMap<>();
                for (WordBlock wordBlock : filteredWordBlocks) {
                    filteredWordBlocksMap.put(wordBlock.word, wordBlock);
                }

                // Check if docId has valid phrases
                PageFileChannel positionalChannel = this.getSegmentChannel(i, "positions");
                List<Integer> validDocIds = new ArrayList<>();
                for (Integer docId : intersection) {
                    // Store current valid phrase positions
                    List<Integer> validPosition = null;
                    // For each word, find the docId's positional list
                    for (String word : analyzed) {
                        ListBlock listBlock = this.getListBlockFromSegment(listChannel, filteredWordBlocksMap.get(word));

                        // Read position list for this word in this docId
                        List<Integer> offsetList = listBlock.globalOffsets;
                        int index = listBlock.invertedList.indexOf(docId);
                        List<Integer> position = this.getPositionList(positionalChannel, offsetList, index);

                        // Continue If no position in this document( which is impossible but just in case)
                        if (position == null || position.size() == 0)
                            continue;

                        // Update validPosition
                        if (validPosition == null)
                            validPosition = new ArrayList<>(position);
                        else {
                            List<Integer> newValid = new ArrayList<>();
                            int leftBound = 0;
                            // find every pair of consecutive position
                            for (int positionA : validPosition) {    // positionA : previous word
                                // Binary search
                                int left = leftBound, right = position.size() - 1;
                                while (left < right) {
                                    int mid = (left + right + 1) / 2; //right bias
                                    if (position.get(mid) <= positionA) {
                                        left = mid;
                                    } else {
                                        right = mid - 1;
                                    }
                                }
                                // Add valid position  todo : check validity
                                if (left == leftBound && position.get(left) == positionA + 1) {
                                    newValid.add(position.get(left));
                                    leftBound = left;
                                } else if (left < position.size() - 1 && position.get(left + 1) == positionA + 1) {  //position.get(left) <= A
                                    newValid.add(position.get(left + 1));
                                    leftBound = left + 1;
                                } else {
                                    if (position.get(left) <= positionA)
                                        leftBound += 1;
                                }
                            }
                            // update the positions
                            validPosition = newValid;
                        }
                        // Break check loop for this docID when no valid position remains
                        if (validPosition == null || validPosition.size() == 0)
                            break;
                    }

                    if (validPosition != null && validPosition.size() > 0)
                        validDocIds.add(docId);
                }

                //read doc
                for (int docId : validDocIds) {
                    documents.add(documentStore.getDocument(docId));
                }

                documentStore.close();
                wordsChannel.close();
                listChannel.close();
                positionalChannel.close();
            }
            i++;
        }

        return documents.iterator();
    }

    /**
     * Performs top-K ranked search using TF-IDF.
     * Returns an iterator that returns the top K documents with highest TF-IDF scores.
     * <p>
     * Each element is a pair of <Document, Double (TF-IDF Score)>.
     * <p>
     * If parameter `topK` is null, then returns all the matching documents.
     * <p>
     * Unlike Boolean Query and Phrase Query where order of the documents doesn't matter,
     * for ranked search, order of the document returned by the iterator matters.
     *
     * @param topK, number of top documents weighted by TF-IDF, all documents if topK is null
     * @return a iterator of ordered documents matching the query
     */
    public Iterator<Pair<Document, Double>> searchTfIdf(List<String> keywords, Integer topK) { // todo:modify the return .
        // queue< Pair<score,  DocID>  // DocID is <SegmentID, LocalDocID>
        PriorityQueue<Pair<Double, DocID>> priorityQueue = new PriorityQueue<>((o1, o2) -> {
            double res = o1.getLeft() - o2.getLeft();
            if (res == 0)
                return 0;
            else return res > 0 ? -1 : 1;   // Decreasing order queue.
        });

        // Analyze phrase words
        ArrayList<String> analyzed = new ArrayList<>();
        for (String keyword : keywords) {
            List<String> result = this.analyzer.analyze(keyword);
            // add possible keywords
            if (result != null && result.size() > 0 && !result.get(0).equals(""))
                analyzed.addAll(result);
        }

        Set<String> uniqueTerms = new HashSet<>(analyzed);

        // Pass 1: get each word's document frequency; and overall document num
        int globalDocNum = 0;
        // Map< word, documentAmount >
        Map<String, Integer> documentFrequency = new HashMap<>();
        int segNum = 0;
        while (true) {
            if (!Files.exists(basePath.resolve("segment" + segNum + "_words"))) {
                break;
            } else {
                // int df = this.getDocumentFrequency(segNum, analyzed[j])
                // Accumulate overall doc Num:
                globalDocNum += this.getNumDocuments(segNum);
                // Open words list, search word
                PageFileChannel wordPage = this.getSegmentChannel(segNum, "words");
                PageFileChannel listPage = this.getSegmentChannel(segNum, "lists");

                List<WordBlock> wordBlockList = this.getWordBlocksFromSegment(wordPage, segNum);
                // Filter analyzed words
                for (WordBlock wordBlock : wordBlockList) {
                    if (uniqueTerms.contains(wordBlock.word)) {
                        ListBlock listBlock = this.getListBlockFromSegment(listPage, wordBlock);
                        int originDocNum = documentFrequency.getOrDefault(wordBlock.word, 0);
                        documentFrequency.put(wordBlock.word, originDocNum + listBlock.invertedList.size());
                    }
                }

                // close pages.
                wordPage.close();
                listPage.close();
            }
            segNum++;
        }

        // calculate query's tf-idf vector
        // Map < term, tfidf >
        Map<String, Double> queryVector = new HashMap<>();
        // Calc term frequency
        for (String term : analyzed) {
            // Get oldVal : Term may duplicates
            double origin = queryVector.getOrDefault(term, 0.0);
            // accumulate one more "idf"
            double newVal = origin + globalDocNum / (double) documentFrequency.getOrDefault(term, 0);  // usually doc freq is not 0
            queryVector.put(term, newVal);
        }

        // Pass 2: get each doc's term frequency, get tf-idf, then multiply with queue vector element by element and do cumulation
        /**
         * Map<DocID, Double> dotProductAccumulator; //  DocID is <SegmentID, LocalDocID>
         * Map<DocID, Double> vectorLengthAccumulator;
         *
         * for each segment:
         *   for each query token w:
         *     for each docID on the postingList of w:
         *       tfidf = TF(w, docID) * IDF(w);
         *       dotProductAccumulator[docID] += tfidf * queryTfIdf[w];
         *       vectorLengthAccumulator[docID] += tfidf ^ 2;
         *
         *   for each docID in this segment
         *     score(docID) =  dotProductAccumulator[docID] / sqrt(vectorLengthAccumulator[docID]);
         */
        segNum = 0;
        while (true) {
            if (!Files.exists(basePath.resolve("segment" + segNum + "_words"))) {
                break;
            } else {
                PageFileChannel wordPage = this.getSegmentChannel(segNum, "words");
                PageFileChannel listPage = this.getSegmentChannel(segNum, "lists");

                Map<DocID, Double> dotProductAccumulator = new HashMap<>();
                Map<DocID, Double> vectorLengthAccumulator = new HashMap<>();
                Map<DocID, Double> scores = new HashMap<>();

                // Cumulate doc info
                List<WordBlock> wordBlockList = this.getWordBlocksFromSegment(wordPage, segNum);
                for (WordBlock wordBlock : wordBlockList) {
                    if (uniqueTerms.contains(wordBlock.word)) {
                        String term = wordBlock.word;
                        ListBlock listBlock = this.getListBlockFromSegment(listPage, wordBlock);

                        // For each docID, accumulate the product
                        for (int index = 0; index < listBlock.invertedList.size(); index++) {
                            int docId = listBlock.invertedList.get(index);
//                            PageFileChannel positionPage = this.getSegmentChannel(segNum, "positions");
//                            List<Integer> positionList = this.getPositionList(positionPage, listBlock.globalOffsets, index); // term freq//todo check global offset
                            int positionListSize = listBlock.sizeList.get(index);
                            // Calc tfidf
                            double tfidf = positionListSize * (globalDocNum / (double) documentFrequency.getOrDefault(term, 0));
                            // Update doc * query ; Update (doc)^2
                            DocID curDoc = new DocID(segNum, docId);
                            double oldProduct = dotProductAccumulator.getOrDefault(curDoc, 0.0);
                            dotProductAccumulator.put(curDoc, oldProduct + tfidf * queryVector.get(term));
                            double oldLength = vectorLengthAccumulator.getOrDefault(curDoc, 0.0);
                            vectorLengthAccumulator.put(curDoc, oldLength + tfidf * tfidf);
                        }
                    }
                }

                // Conclude scores for documents:
                for (DocID docId : dotProductAccumulator.keySet()) {
                    double sc = dotProductAccumulator.get(docId) / Math.sqrt(vectorLengthAccumulator.get(docId));
                    scores.put(docId, sc);
                    priorityQueue.add(new Pair<>(sc, docId));
                    // Keep queue size in range of K // if topK == 0 , skip polling
                    if (topK > 0 && priorityQueue.size() > topK) {
                        priorityQueue.poll();
                    }
                }

                wordPage.close();
                listPage.close();
            }
            segNum++;
        }

        // Pass 3: retrieve doc ID from pair; reorganize docID sequence
        List<Pair<Double, DocID>> topDocs = new ArrayList<>();
        while (!priorityQueue.isEmpty()) {
            Pair<Double, DocID> pair = priorityQueue.poll();
            if (priorityQueue.size() < topK)
                topDocs.add(pair);
        }

        List<Pair<Document, Double>> result = new ArrayList<>();
        // TODO: document store size?
        // TODO: document store num?
        DocumentStore documentStore = this.getDocumentStore(segNum - 1, "");
        for (int i = 0; i < topDocs.size(); i++) {
            Pair<Double, DocID> pair = topDocs.get(i);
            int seg = pair.getRight().segmentID;
            int locID = pair.getRight().localID;
            result.add(new Pair<>(documentStore.getDocument(locID), pair.getLeft()));
        }

        return result.iterator();
    }

    /**
     * Returns the total number of documents within the given segment.
     */
    public int getNumDocuments(int segmentNum) {
        DocumentStore documentStore = this.getDocumentStore(segmentNum, "");
        int result = (int) documentStore.size();
        documentStore.close();
        return result;
    }

    /**
     * Returns the number of documents containing the token within the given segment.
     * The token should be already analyzed by the analyzer. The analyzer shouldn't be applied again.
     */
    public int getDocumentFrequency(int segmentNum, String token) {
        // open words list, search word -> [word] : get posting list size (No. of doc)
        PageFileChannel wordPage = this.getSegmentChannel(segmentNum, "words");
        PageFileChannel listPage = this.getSegmentChannel(segmentNum, "lists");

        List<WordBlock> wordBlockList = this.getWordBlocksFromSegment(wordPage, segmentNum);
        // Search for token
        for (WordBlock wordBlock : wordBlockList) {
            if (wordBlock.word.equals(token)) {
                // Read posting list and get size
                ListBlock listBlock = this.getListBlockFromSegment(listPage, wordBlock);
                // close pages.
                wordPage.close();
                listPage.close();
                return listBlock.invertedList.size();
            }
        }

        // close pages.
        wordPage.close();
        listPage.close();

        return 0;
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
        // Add to memory
        this.deletedWords.add(keyword);
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
            ListBlock listBlock = this.getListBlockFromSegment(listsFileChannel, wordBlock);

            invertedListsForTest.put(wordBlock.word, listBlock.invertedList);
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


    /**
     * Reads a disk segment of a positional index into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     * <p>
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public PositionalIndexSegmentForTest getIndexSegmentPositional(int segmentNum) {
        PageFileChannel listsFileChannel = this.getSegmentChannel(segmentNum, "lists");
        PageFileChannel wordsFileChannel = this.getSegmentChannel(segmentNum, "words");
        PageFileChannel posFileChannel = this.getSegmentChannel(segmentNum, "positions");

        // Get all word blocks
        List<WordBlock> wordBlocks = this.getWordBlocksFromSegment(wordsFileChannel, segmentNum);

        Map<String, List<Integer>> invertedListsForTest = new HashMap<>();
        Table<String, Integer, List<Integer>> positionsListsForTest = HashBasedTable.create();

        for (WordBlock wordBlock : wordBlocks) {
            ListBlock listBlock = this.getListBlockFromSegment(listsFileChannel, wordBlock);
            for (int i = 0; i < listBlock.invertedList.size(); i++) {
                // Get document Id
                int docId = listBlock.invertedList.get(i);
                // Decode position list
                List<Integer> positionList = this.getPositionList(posFileChannel, listBlock.globalOffsets, i);

                // Add to table
                positionsListsForTest.put(wordBlock.word, docId, positionList);
            }
            invertedListsForTest.put(wordBlock.word, listBlock.invertedList);
        }

        listsFileChannel.close();
        wordsFileChannel.close();
        posFileChannel.close();

        Map<Integer, Document> documentsForTest = this.getDocumentsForTest(segmentNum);

        return documentsForTest.size() != 0 ?
                new PositionalIndexSegmentForTest(invertedListsForTest, documentsForTest, positionsListsForTest) : null;
    }

    private List<Integer> getPositionList(PageFileChannel posFileChannel, List<Integer> globalOffsets, int currentIndex) {
        // Calculate position list meta
        int globalOffset = globalOffsets.get(currentIndex);
        int pageNum = globalOffset / PageFileChannel.PAGE_SIZE;
        int posOffset = globalOffset % PageFileChannel.PAGE_SIZE;
        // Get position list length
        int posLength = globalOffsets.get(currentIndex + 1) - globalOffsets.get(currentIndex);
        // Get position list
        byte[] encodedPositionList = new byte[posLength];
        ByteBuffer posReadBuffer = posFileChannel.readPage(pageNum);
        posReadBuffer.position(posOffset);
        for (int j = 0; j < posLength; j++) {
            if (posReadBuffer.position() >= posReadBuffer.capacity()) {
                // Read next page
                pageNum += 1;
                posReadBuffer = posFileChannel.readPage(pageNum);
            }
            encodedPositionList[j] = posReadBuffer.get();
        }
        // Decode position list
        return this.compressor.decode(encodedPositionList);
    }
}
