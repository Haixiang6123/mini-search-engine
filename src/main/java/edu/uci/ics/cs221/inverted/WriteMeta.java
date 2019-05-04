package edu.uci.ics.cs221.inverted;

public class WriteMeta {
    public int listsPageOffset = 0;
    public int listsPageNum = 0;
    public int wordsPageNum = 0;
    public int originListsPageNum = 0;

    public void reset() {
        this.listsPageOffset = 0;
        this.listsPageNum = 0;
        this.wordsPageNum = 0;
        this.originListsPageNum = 0;
    }
}
