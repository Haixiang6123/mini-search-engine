package edu.uci.ics.cs221.index.inverted;

import javax.print.Doc;

public class DocID {
    final int segmentID;
    final int localID;

    public DocID(int segNum, int docNum) {
        this.segmentID = segNum;
        this.localID = docNum;
    }

    @Override
    public int hashCode() {
        return (this.segmentID << 16) + localID;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DocID)) return false;
        if (((DocID) obj).segmentID != this.segmentID) return false;
        if (((DocID) obj).localID != this.localID) return false;
        return true;
    }
}
