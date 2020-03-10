package BigT;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import global.PageId;
import global.MID;
import global.AttrType;
import BigT.Stream;

import java.io.IOException;
import java.io.File;
import java.util.Arrays;

public class bigT {

    private int type;
    private String name;
    private String indexNames[];
    protected Heapfile heapfile;



    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    void bigT(String name, int type) throws HFException, IOException, HFDiskMgrException, InvalidPageNumberException, DiskMgrException, FileIOException, HFBufMgrException {
        this.type = type;
        this.name = name;

        try {

            this.heapfile = new Heapfile(name + ".hfile");
            createIndex();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
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


    public Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter){

        try {
            switch (orderType) {
                case 1:
                    //results are row, col, ts
                    break;
                case 2:
                    //col, row, ts
                    break;
                case 3:
                    //row and ts
                    break;
                case 4:
                    //col, ts
                    break;
                case 5:
                    //TS
                    break;
                default:
                    throw new Exception("Invalid OrderType Passed");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

}