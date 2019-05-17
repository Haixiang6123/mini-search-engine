package edu.uci.ics.cs221.inverted;

import java.util.ArrayList;
import java.util.List;

public class WordBlock {
    public int wordLength = 0;
    public String word = "";
    public int listsPageNum = 0;
    public int listOffset = 0;
    public int listLength = 0;
    public int posOffset = 0;

    public int segment = 0;

    public WordBlock(int wordLength, String word, int listsPageNum, int listOffset, int listLength, int posOffset) {
        this.wordLength = wordLength;
        this.word = word;
        this.listsPageNum = listsPageNum;
        this.listOffset = listOffset;
        this.listLength = listLength;
        this.posOffset = posOffset;
    }

    public int getWordBlockCapacity() {
        return Integer.BYTES + this.wordLength + Integer.BYTES + Integer.BYTES + Integer.BYTES;
    }

    @Override
    public String toString() {
        return "WordLength: " + this.wordLength + "; " +
                "Word: " + this.word + "; " +
                "ListsPageNum: " + this.listsPageNum + "; " +
                "ListOffset: " + this.listOffset + "; " +
                "ListLength: " + this.listLength + "; " +
                "PosOffset: " + this.posOffset;
    }

    public boolean equals(Object object){
        if (object == this) {
            return true;
        }

        if (!(object instanceof WordBlock)) {
            return false;
        }
        WordBlock wordBlock = (WordBlock) object;
        return this.word.equals(wordBlock.word);
    }
}
