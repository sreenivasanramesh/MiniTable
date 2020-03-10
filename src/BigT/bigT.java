package BigT;

import btree.BTreeFile;
import btree.DeleteFashion;
import btree.StringKey;
import global.*;
import heap.Heapfile;

import java.io.File;

import static global.GlobalConst.NUMBUF;

//import static BigT.InstrumentationAgent.getObjectSize;

public class bigT {

    int type;
    String name;
    String[] indexNames;
    Heapfile heapfile;

    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    public bigT(String name, int type) throws Exception {
        this.type = type;
        this.name = name;
        System.out.println("Calling Create Index");
        createIndex();
    }

    public static void main(String[] args) throws Exception {
        new SystemDefs("/Users/rakeshr/test.db", 10, NUMBUF, "Clock");

        bigT bigT = new bigT("test1", 2);
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

    private void createIndex() throws Exception {
        switch (this.type) {
            case 1:
                this.indexNames = new String[]{};
                break;
            case 2:
                String indexFileName = this.name + "_row.idx";
                File indexFile = new File(indexFileName);
                if (!indexFile.exists()) {

                    BTreeFile bTreeFile = new BTreeFile("/Users/rakeshr/" + indexFileName, AttrType.attrString, 4, DeleteFashion.NAIVE_DELETE);
                    StringKey str = new StringKey("test1");
                    bTreeFile.insert(new StringKey("t"), new RID(new PageId(1), 0));
                }
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
//        for (String indexName : this.indexNames) {
//            File file = new File(indexName);
//            if (!file.exists()) {
//                String[] tempArray = indexName.substring(0, indexName.lastIndexOf('.')).split("_");
//                String[] indexArray = Arrays.copyOfRange(tempArray, 1, tempArray.length);
//                BTreeFile bTreeFile = new BTreeFile(indexName);
//                for (String index : indexArray) {
//                    // Btree Index to create Index
//                }
//            }
//        }
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