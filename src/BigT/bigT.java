package BigT;

import global.MID;

public class bigT {

    int type;

    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    void bigT(String name, int type) {
        this.type = type;
    }

    //Delete the bigtable from the database.
    void deleteBigt() {

    }

    // Return number of maps in the bigtable.
    int getMapCnt() {

        return 0;
    }

    // Return number of distinct row labels in the bigtable.
    int getRowCnt() {
        return 0;
    }

    //    Return number of distinct column labels in the bigtable.
    int getColumnCnt() {

        return 0;
    }

    // TODO: insert and return MID
    MID insertMap(byte[] mapPtr) {
        return new MID();
    }

}