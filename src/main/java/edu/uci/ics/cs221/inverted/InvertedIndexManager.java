package edu.uci.ics.cs221.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import utils.Utils;

import javax.print.Doc;
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
 *
 * Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
     *
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;

    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     *
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    // Native analyzer
    private Analyzer analyzer = null;
    // Native Page File Channel
    private PageFileChannel pageFileChannel = null;
    // In-memory data structure for storing inverted index
    private Map<String, List<Integer>> invertedIndex = null;
    // Base directory
    private Path basePath = null;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.basePath = Paths.get(indexFolder);
        this.invertedIndex = new HashMap<>();
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
     * @param document
     */
    public void addDocument(Document document) {
        List<String> tokens = this.analyzer.analyze(document.getText());
        for (String token : tokens) {
            List<Integer> indexes = invertedIndex.get(token);
            if (indexes == null) {
                // Create a new list
                invertedIndex.put(token, new ArrayList<>(Collections.singletonList(document.hashCode())));
            }
            else {
                // Add to exist list
                indexes.add(document.hashCode());
            }
        }
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        String str = "hello world";
        System.out.println(str.getBytes().length);
        ByteBuffer buffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        buffer.put(str.getBytes());
        pageFileChannel = PageFileChannel.createOrOpen(basePath.resolve("segment3"));
        pageFileChannel.writePage(str.getBytes().length / PageFileChannel.PAGE_SIZE, buffer);
        pageFileChannel.close();
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
                pageFileChannel = PageFileChannel.createOrOpen(basePath.resolve("segment" + i));
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
                pageFileChannel = PageFileChannel.createOrOpen(basePath.resolve("segment" + i));
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
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
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
