package BigT;

import cmdline.MiniTable;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Heapfile;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        Map newMap = tempMap;
        String tempRow = tempMap.getRowLabel();
        System.out.println("Left Stream results => ");
        while (tempMap!= null) {
            if (!tempMap.getRowLabel().equals(tempRow)) {
                this.leftHeapFile.insertMap(newMap.getMapByteArray());
            }
            tempRow = tempMap.getRowLabel();
            newMap = tempMap;
            tempMap = this.leftStream.getNext();
        }
        this.leftHeapFile.insertMap(newMap.getMapByteArray());
        System.out.println("left count = " + this.leftHeapFile.getRecCnt());
        leftStream.closeStream();
    }


    public void storeRightColMatch() throws Exception {
        Map tempMap = this.rightStream.getNext();
        Map newMap = tempMap;
        String tempRow = tempMap.getRowLabel();
        System.out.println("Right Stream results => ");
        while (tempMap!= null) {
            if (!tempMap.getRowLabel().equals(tempRow)) {
                this.rightHeapFile.insertMap(tempMap.getMapByteArray());
            }
            tempRow = tempMap.getRowLabel();
            newMap = tempMap;
            tempMap = this.rightStream.getNext();
        }
        this.rightHeapFile.insertMap(newMap.getMapByteArray());
        System.out.println("right count = " + this.rightHeapFile.getRecCnt());
        rightStream.closeStream();
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
                    4, MiniTable.BIGT_STR_SIZES, 3, 4, 3,4,this.NUM_BUF,
                    this.leftIterator, this.rightIterator, false, false, sortOrder, outFilter,
                    projection, 1);
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public void StoreJoinResult() throws Exception {
        Map tempMap = sm.get_next();
        System.out.println("Store Join Results = " + tempMap);
        while (tempMap != null) {
            tempMap.print();
            storeToBigT(tempMap.getRowLabel(), tempMap.getColumnLabel());
            tempMap = sm.get_next();
        }
        sm.close();
    }

    public static Map getJoinMap(String rowKey, String columnKey, String value, Integer timestamp) {

        short[] attrSizes = new short[3];
        attrSizes[0] = (short) (MiniTable.BIGT_STR_SIZES[0]*2 + 1);
        attrSizes[1] = (short) (MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[1] + 1);
        attrSizes[2] = MiniTable.BIGT_STR_SIZES[2];

        Map map = new Map();
        try {
            map.setHeader(MiniTable.BIGT_ATTR_TYPES, attrSizes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map map1 = new Map(map.size());
        try {
            map1.setHeader(MiniTable.BIGT_ATTR_TYPES, attrSizes);
            map1.setRowLabel(rowKey);
            map1.setColumnLabel(columnKey);
            map1.setTimeStamp(timestamp);
            map1.setValue(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map1;
    }


    public void storeToBigT(String leftRowLabel, String rightRowLabel) throws Exception {
        // TODO: set self bigTName
        List<Map> joinedMaps = new ArrayList<>();
        String bigTName = "dummy";
        String JOIN_BT_NAME = leftRowLabel + rightRowLabel;
        resultantBigT = new bigT(this.outBigTName);
        Stream tempStream = new bigT(bigTName).openStream(0, leftRowLabel, "*", "*");
        Map tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (tempMap.getColumnLabel().equals(this.columnName)) {
                joinedMaps.add(tempMap);
            } else {
                String rowLabel = leftRowLabel + ":" + rightRowLabel;
                String columnLabel = leftRowLabel + ":" + tempMap.getColumnLabel();
                String ValueLabel = tempMap.getValue();
                Integer timeStampVal = tempMap.getTimeStamp();

                Map tempMap2 = getJoinMap(rowLabel, columnLabel, ValueLabel, timeStampVal);
                resultantBigT.insertMap(tempMap2.getMapByteArray());
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closeStream();


        tempStream = new bigT(bigTName).openStream(0, rightRowLabel, "*", "*");
        tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (tempMap.getColumnLabel().equals(this.columnName)) {
                joinedMaps.add(tempMap);
            } else {
                String rowLabel = leftRowLabel + ":" + rightRowLabel;
                String columnLabel = leftRowLabel + ":" + tempMap.getColumnLabel();
                String ValueLabel = tempMap.getValue();
                Integer timeStampVal = tempMap.getTimeStamp();

                Map tempMap2 = getJoinMap(rowLabel, columnLabel, ValueLabel, timeStampVal);
                resultantBigT.insertMap(tempMap2.getMapByteArray());
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closeStream();

        // Remove duplicates
        getFirstThree(joinedMaps);
        for (Map tempMap3 : joinedMaps) {
            String rowLabel = leftRowLabel + ":" + rightRowLabel;
            String columnLabel = leftRowLabel + ":" + tempMap3.getColumnLabel();
            String ValueLabel = tempMap3.getValue();
            Integer timeStampVal = tempMap3.getTimeStamp();

            Map tempMap4 = getJoinMap(rowLabel, columnLabel, ValueLabel, timeStampVal);
            resultantBigT.insertMap(tempMap4.getMapByteArray());
        }
    }

    public void getFirstThree(List<Map> mapList) throws Exception {
        if (mapList.size() <= 3) {
            return;
        }

        do {
            int minIndex = 0;
            int minTimeStamp = mapList.get(0).getTimeStamp();
            for (int i = 1; i < mapList.size(); i++) {
                if (mapList.get(i).getTimeStamp() < minTimeStamp) {
                    minTimeStamp = mapList.get(i).getTimeStamp();
                    minIndex = i;
                }
            }

            mapList.remove(minIndex);
        } while (mapList.size() != 3);

    }

    public void cleanUp() throws Exception {
        this.leftHeapFile.deleteFile();
        this.rightHeapFile.deleteFile();
    }
}
