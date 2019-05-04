package edu.uci.ics.cs221.inverted;

import java.util.ArrayList;
import java.util.List;

public class MergedWordBlock {
    public boolean isSingle = true;
    public WordBlock leftWordBlock = null;
    public WordBlock rightWordBlock = null;

    public List<Integer> segments = new ArrayList<>();

    public MergedWordBlock(boolean isSingle) {
        this.isSingle = isSingle;
    }
}
