package BigT;

import cmdline.MiniTable;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Heapfile;
import iterator.*;

public class rowJoin {
    private String columnName;
    private int NUM_BUF;
    private bigT rightBigT, resultantBigT;
    private Stream leftStream, rightStream;
    private Heapfile leftHeapFile;
    private Heapfile rightHeapFile;
    private String LEFT_HEAP = "leftTempHeap";
    private String RIGHT_HEAP = "rightTempHeap";
    private TupleOrder sortOrder = new TupleOrder(TupleOrder.Ascending);
    private SortMerge sm = null;
    private FileScan leftIterator, rightIterator;
    private String outBigTName;


    public rowJoin(int amt_of_mem, Stream leftStream, String RightBigTName, String ColumnName, String outBigTName)  throws Exception {
        this.columnName = ColumnName;
        this.NUM_BUF = amt_of_mem;
        this.rightBigT = new bigT(RightBigTName);
        this.leftStream = leftStream;
        // Left stream should be filtered on column
        this.rightStream = this.rightBigT.openStream(1, "*", this.columnName, "*");
        this.leftHeapFile = new Heapfile(LEFT_HEAP);
        this.rightHeapFile = new Heapfile(RIGHT_HEAP);
        this.outBigTName = outBigTName;

        storeLeftColMatch();
        storeRightColMatch();
        SortMergeJoin();
        StoreJoinResult();
        cleanUp();
        //return new Stream(new bigT(this.outBigTName), 1, "*", "*", "*");
    }


    public void storeLeftColMatch() throws Exception {
        Map tempMap = this.leftStream.getNext();
        while (tempMap!= null) {
//            if(tempMap.getColumnLabel().equals(this.columnName)) {
//                matchingMap = tempMap;
//                this.leftHeapFile.insertMap(matchingMap.getMapByteArray());
//            }
            this.leftHeapFile.insertMap(tempMap.getMapByteArray());
            tempMap = this.leftStream.getNext();
        }
        leftStream.closeStream();
        // Now we have all maps with that matches the column label
    }

    public void storeRightColMatch() throws Exception {
        Map tempMap = this.rightStream.getNext();
        while (tempMap!= null) {
            this.rightHeapFile.insertMap(tempMap.getMapByteArray());
            tempMap = this.rightStream.getNext();
        }
        rightStream.closeStream();
        // Now we have two heapfiles with same column names
    }

    public void SortMergeJoin() throws Exception {
        MapIterator leftIterator, rightIterator;

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 4);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
        outFilter[1] = null;

        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        try {
            this.leftIterator = new FileScan(LEFT_HEAP, MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
            this.rightIterator = new FileScan(RIGHT_HEAP, MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
            this.sm = new SortMerge(MiniTable.BIGT_ATTR_TYPES, 4, MiniTable.BIGT_STR_SIZES, MiniTable.BIGT_ATTR_TYPES,
                    4, MiniTable.BIGT_STR_SIZES, 4, 4, 4,4,this.NUM_BUF,
                    this.leftIterator, this.rightIterator, false, false, sortOrder, outFilter,
                    projection, 1);
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public void StoreJoinResult() throws Exception {
        Map tempMap = sm.get_next();
        while (tempMap != null) {
            storeToBigT(tempMap.getRowLabel(), tempMap.getColumnLabel());
            tempMap = sm.get_next();
        }
    }

    public void storeToBigT(String leftRowLabel, String rightRowLabel) throws Exception {
        // TODO: set self bigTName
        String bigTName = "dummy";
        String JOIN_BT_NAME = leftRowLabel + rightRowLabel;
        resultantBigT = new bigT(this.outBigTName);
        Stream tempStream = new bigT(bigTName).openStream(0, leftRowLabel, "*", "*");
        Map tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (tempMap.getColumnLabel().equals(this.columnName)) {
                resultantBigT.insertMap(tempMap.getMapByteArray());
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closeStream();
        tempStream = new bigT(bigTName).openStream(0, rightRowLabel, "*", "*");
        tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (tempMap.getColumnLabel().equals(this.columnName)) {
                resultantBigT.insertMap(tempMap.getMapByteArray());
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closeStream();
    }

    public void cleanUp() throws Exception {
        this.sm.close();
        this.leftHeapFile.deleteFile();
        this.rightHeapFile.deleteFile();
    }
}
