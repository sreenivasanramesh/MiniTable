package BigT;

import btree.BTreeFile;
import btree.DeleteFashion;
import btree.StringKey;
import global.AttrType;
import global.MID;
import global.PageId;
import global.SystemDefs;
import heap.Heapfile;

import java.io.File;

import static global.GlobalConst.NUMBUF;

//import static BigT.InstrumentationAgent.getObjectSize;

public class bigT {

    int type;
    String name;
    String[] indexNames;


    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    public bigT(String name, int type) throws Exception {
        this.type = type;
        this.name = name;
        System.out.println("Calling Create Index");
        createIndex();
    }

    public static void main(String[] args) throws Exception {
        boolean isNewDb = false;
        int numPages = isNewDb ? 10 : 0;
        new SystemDefs("/Users/sumukhashwinkamath/test.db", numPages, NUMBUF, "LRU");
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
                    Map map = new Map();
                    short[] strSizes = new short[]{(short) "1".getBytes().length, (short) "2".getBytes().length, (short) "4".getBytes().length};
                    map.setHeader(new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)}, strSizes);
                    map.setRowLabel("1");
                    map.setColumnLabel("2");
                    map.setTimeStamp(3);
                    map.setValue("4");
                    Map map1 = new Map();
                    short[] strSizes1 = new short[]{(short) "a".getBytes().length, (short) "b".getBytes().length, (short) "d".getBytes().length};
                    map1.setHeader(new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)}, strSizes1);
                    map1.setRowLabel("a");
                    map1.setColumnLabel("b");
                    map1.setTimeStamp(6);
                    map1.setValue("d");
                    Heapfile hf = new Heapfile(this.name + "3.heap");
//                    MID mid = hf.insertMap(map.getMapByteArray());
//                    MID mid2 = hf.insertMap(map1.getMapByteArray());
//
//                    System.out.println("mid = " + mid);
//                    System.out.println("mid = " + mid.getPageNo());
//                    System.out.println("mid = " + mid.getSlotNo());
//                    System.out.println("mid2 = " + mid2);
//                    System.out.println("mid2 = " + mid2.getPageNo());
//                    System.out.println("mid2 = " + mid2.getSlotNo());
//                    BTreeFile bTreeFile = new BTreeFile("/Users/rakeshr/" + indexFileName, AttrType.attrString, 4, DeleteFashion.NAIVE_DELETE);
                    StringKey str = new StringKey("test1");
//                    RID rid = new RID(mid.getPageNo(), mid.getSlotNo());
//                    RID rid2 = new RID(mid.getPageNo(), mid.getSlotNo());
                    MID mid = new MID(new PageId(7), 0);
                    MID mid2 = new MID(new PageId(7), 1);
//                    bTreeFile.insert(new StringKey("1"), rid);
//                    bTreeFile.insert(new StringKey("a"), rid2);

                    System.out.println("hf.getRecCnt() = " + hf.getRecCnt());
                    System.out.println("MAPAPAPPAA = " + hf.getMap(mid));

                    Map la = hf.getMap(mid);
                    Map la2 = hf.getMap(mid2);

                    la.setHeader(new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)}, strSizes);

                    la2.setHeader(new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)}, strSizes1);
                    la.print();
                    la2.print();
                    SystemDefs.JavabaseBM.flushAllPages();
                    SystemDefs.JavabaseDB.closeDB();

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
        return null;
    }

}