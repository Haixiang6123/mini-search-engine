package edu.uci.ics.cs221.index.inverted;

import java.util.List;

public class ListBlock {
    public byte[] encodedInvertedList = null;
    public byte[] encodedGlobalOffsets = null;
    public List<Integer> invertedList = null;
    public List<Integer> globalOffsets = null;

    public ListBlock(int listLength, int globalOffsetLength) {
        this.encodedInvertedList = new byte[listLength];
        this.encodedGlobalOffsets = new byte[globalOffsetLength];
    }
}
