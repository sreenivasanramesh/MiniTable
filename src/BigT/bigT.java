package BigT;

import global.MID;

import java.io.File;
import java.util.Arrays;

public class bigT {

    int type;
    String name;
    String indexNames[];


    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    public void bigT(String name, int type) throws Exception {
        this.type = type;
        this.name = name;
        createIndex();
    }

    private void createIndex() throws Exception {
        switch (this.type) {
            case 1:
                this.indexNames = new String[]{};
                break;
            case 2:
                this.indexNames = new String[]{this.name + "_row.idx"};
                break;
            case 3:
                this.indexNames = new String[]{this.name + "_column.idx"};
                break;
            case 4:
                this.indexNames = new String[]{this.name + "_column_row.idx", this.name + "_timestamp.idx"};
                break;
            case 5:
                this.indexNames = new String[]{this.name + "_row_value.idx", this.name + "_timestamp.idx"};
                break;
            default:
                throw new Exception("Invalid Type Passed");
        }
        for (String indexName : this.indexNames) {
            File file = new File(indexName);
            if (!file.exists()) {
                String[] tempArray = indexName.substring(0, indexName.lastIndexOf('.')).split("_");
                String[] indexArray = Arrays.copyOfRange(tempArray, 1, tempArray.length);
                for (String index : indexArray) {
                    // Btree Index to create Index
                }
            }
        }
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