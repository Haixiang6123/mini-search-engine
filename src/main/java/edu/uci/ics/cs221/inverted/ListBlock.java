package edu.uci.ics.cs221.inverted;

public class ListBlock {
    public final static int CAPACITY = Integer.BYTES * 4;

    public int docId = 0;
    public int listsPageNum = 0;
    public int listOffset = 0;
    public int listLength = 0;

    public int segment = 0;

    public ListBlock(int docId, int listsPageNum, int listOffset, int listLength) {
        this.docId = docId;
        this.listsPageNum = listsPageNum;
        this.listOffset = listOffset;
        this.listLength = listLength;
    }
}
