package edu.uci.ics.cs221.inverted;

public class WordBlock {
    public int wordLength = 0;
    public String word = "";
    public int listsPageNum = 0;
    public int listOffset = 0;
    public int listLength = 0;

    public WordBlock(int wordLength, String word, int listsPageNum, int listOffset, int listLength) {
        this.wordLength = wordLength;
        this.word = word;
        this.listsPageNum = listsPageNum;
        this.listOffset = listOffset;
        this.listLength = listLength;
    }

    public int getWordBlockCapacity() {
        return Integer.BYTES + this.wordLength + Integer.BYTES + Integer.BYTES + Integer.BYTES;
    }
}
