package edu.uci.ics.cs221.inverted;

import java.nio.ByteBuffer;

public class WriteMeta {
    public int listsPageOffset = 0;
    public int listsPageNum = 0;
    public int positionPageOffset = 0;
    public int positionPageNum = 0;
    public int wordsPageNum = 0;
    public int originListsPageNum = 0;

    public void reset() {
        this.listsPageOffset = 0;
        this.listsPageNum = 0;
        this.wordsPageNum = 0;
        this.positionPageOffset = 0;
        this.positionPageNum = 0;
        this.originListsPageNum = 0;
    }
}
