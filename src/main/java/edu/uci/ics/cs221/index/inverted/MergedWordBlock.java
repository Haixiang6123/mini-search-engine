package edu.uci.ics.cs221.index.inverted;

public class MergedWordBlock {
    public boolean isSingle = true;
    public WordBlock leftWordBlock = null;
    public WordBlock rightWordBlock = null;

    public MergedWordBlock(boolean isSingle) {
        this.isSingle = isSingle;
    }
}
