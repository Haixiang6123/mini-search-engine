package edu.uci.ics.cs221.index.inverted;

import java.util.List;

public class ListBlock {
    public byte[] encodedInvertedList = null;
    public byte[] encodedGlobalOffsets = null;
    public byte[] encodedSizeList = null;
    public List<Integer> invertedList = null;
    public List<Integer> globalOffsets = null;
    public List<Integer> sizeList = null;

    public ListBlock(int listLength, int globalOffsetLength, int sizeLength) {
        this.encodedInvertedList = new byte[listLength];
        this.encodedGlobalOffsets = new byte[globalOffsetLength];
        this.encodedSizeList = new byte[sizeLength];
    }
}
