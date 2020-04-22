package BigT;

import cmdline.MiniTable;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Heapfile;
import iterator.*;

import java.util.ArrayList;
import java.util.List;

public class rowJoin {
    private final String columnName;
    private final int NUM_BUF;
    private bigT resultantBigT;
    private final Stream leftStream;
    private final Stream rightStream;
    private final Heapfile leftHeapFile;
    private final Heapfile rightHeapFile;
    private final String LEFT_HEAP = "leftTempHeap";
    private final String RIGHT_HEAP = "rightTempHeap";
    private final TupleOrder sortOrder = new TupleOrder(TupleOrder.Ascending);
    private SortMerge sortMergeObj = null;
    private final String outBigTName;
    private final String rightBigTName;
    private final String leftBigTName;


    public rowJoin(int amt_of_mem, Stream leftStream, String RightBigTName, String ColumnName, String outBigTName, String leftBigTName)  throws Exception {
        this.columnName = ColumnName;
        this.NUM_BUF = amt_of_mem;
        this.rightBigTName = RightBigTName;
        bigT rightBigT = new bigT(RightBigTName, false);
        this.leftStream = leftStream;
        this.rightStream = rightBigT.openStream(1, "*", columnName, "*");
        this.leftHeapFile = new Heapfile(LEFT_HEAP);
        this.rightHeapFile = new Heapfile(RIGHT_HEAP);
        this.outBigTName = outBigTName;
        this.leftBigTName = leftBigTName;

//        storeLeftColMatch();
        filterDistinctRowMaps(leftStream, leftHeapFile);
        filterDistinctRowMaps(rightStream, rightHeapFile);
//        storeRightColMatch();
        SortMergeJoin();
        StoreJoinResult();
        cleanUp();
        //return new Stream(new bigT(this.outBigTName), 1, "*", "*", "*");
    }


//    public void storeLeftColMatch() throws Exception {
//        Map tempMap = this.leftStream.getNext();
//        Map oldMap = new Map();
//        oldMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
//        oldMap.copyMap(tempMap);
//        String tempRow = tempMap.getRowLabel();
////        System.out.println("Left Stream results => ");
//        while (tempMap!= null) {
//            if (!tempMap.getRowLabel().equals(tempRow)) {
////                oldMap.print();
//                this.leftHeapFile.insertMap(oldMap.getMapByteArray());
//            }
//            tempRow = tempMap.getRowLabel();
//            oldMap.copyMap(tempMap);
//            tempMap = this.leftStream.getNext();
//        }
////        oldMap.print();
//        this.leftHeapFile.insertMap(oldMap.getMapByteArray());
//        System.out.println("left count = " + this.leftHeapFile.getRecCnt());
//
//        this.leftStream.closeStream();
//    }


//    public void storeRightColMatch() throws Exception {
//        Map tempMap = this.rightStream.getNext();
//        Map oldMap = new Map();
//        oldMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
//        oldMap.copyMap(tempMap);
//        String tempRow = tempMap.getRowLabel();
////        System.out.println("Right Stream results => ");
//        while (tempMap!= null) {
//            if (!tempMap.getRowLabel().equals(tempRow)) {
////                oldMap.print();
//                this.rightHeapFile.insertMap(oldMap.getMapByteArray());
//            }
//            tempRow = tempMap.getRowLabel();
//            oldMap.copyMap(tempMap);
//            tempMap = this.rightStream.getNext();
//        }
////        oldMap.print();
//        this.rightHeapFile.insertMap(oldMap.getMapByteArray());
//        System.out.println("right count = " + this.rightHeapFile.getRecCnt());
//
//        this.rightStream.closeStream();
//    }

    public void filterDistinctRowMaps(Stream streamObj, Heapfile heapFileObj) throws Exception {
        Map tempMap = streamObj.getNext();
        Map oldMap = copyMapObj(tempMap);
        String tempRow = tempMap.getRowLabel();
        while (tempMap!= null) {
            if (!tempMap.getRowLabel().equals(tempRow)) {
                heapFileObj.insertMap(oldMap.getMapByteArray());
            }
            tempRow = tempMap.getRowLabel();
            oldMap = copyMapObj(tempMap);
            tempMap = streamObj.getNext();
        }
        heapFileObj.insertMap(oldMap.getMapByteArray());
        System.out.println(streamObj.getBigTName() +" count = " + heapFileObj.getRecCnt());
        streamObj.closeStream();
    }

    public void SortMergeJoin() throws Exception {
        MapIterator leftIterator, rightIterator;

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        outFilter[1] = null;

        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        try {
            FileScan leftIterator1 = new FileScan(LEFT_HEAP, MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
            FileScan rightIterator1 = new FileScan(RIGHT_HEAP, MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
            this.sortMergeObj = new SortMerge(MiniTable.BIGT_ATTR_TYPES, 4, MiniTable.BIGT_STR_SIZES, MiniTable.BIGT_ATTR_TYPES,
                    4, MiniTable.BIGT_STR_SIZES, 3, 4, 3,4,this.NUM_BUF,
                    leftIterator1, rightIterator1, false, false, sortOrder, outFilter,
                    projection, 1);
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public void StoreJoinResult() throws Exception {
        Map tempMap = sortMergeObj.get_next();
        while (tempMap != null) {
            storeToBigT(tempMap.getRowLabel(), tempMap.getColumnLabel());
            tempMap = sortMergeObj.get_next();
        }
        sortMergeObj.close();
    }

    public static Map joinMapRows(String rowKey, String columnKey, String value, Integer timestamp) {

//        short[] attrSizes = new short[3];
//        attrSizes[0] = (short) (MiniTable.BIGT_STR_SIZES[0]*2 + 1);
//        attrSizes[1] = (short) (MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[1] + 1);
//        attrSizes[2] = MiniTable.BIGT_STR_SIZES[2];

        Map map = new Map();
        try {
            map.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map map1 = new Map(map.size());
        try {
            map1.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
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
        List<Map> joinedMaps = new ArrayList<>();
        resultantBigT = new bigT(this.outBigTName, true);
        Stream tempStream = new bigT(leftBigTName, false).openStream(1, leftRowLabel, "*", "*");
        Map tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (!this.columnName.equals(tempMap.getColumnLabel())) {
                String rowLabel = leftRowLabel + ":" + rightRowLabel;
                String columnLabel = leftRowLabel + ":" + tempMap.getColumnLabel();
                String ValueLabel = tempMap.getValue();
                Integer timeStampVal = tempMap.getTimeStamp();
            
                Map tempMap2 = joinMapRows(rowLabel, columnLabel, ValueLabel, timeStampVal);
                tempMap2.print();
                resultantBigT.insertMap(tempMap2.getMapByteArray(), 1);
            } else {
                joinedMaps.add(copyMapObj(tempMap));
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closeStream();
    
    
        tempStream = new bigT(rightBigTName, false).openStream(1, rightRowLabel, "*", "*");
        tempMap = tempStream.getNext();
        while (tempMap != null) {
            if (!this.columnName.equals(tempMap.getColumnLabel())) {
                String rowLabel = leftRowLabel + ":" + rightRowLabel;
                String columnLabel = rightRowLabel + ":" + tempMap.getColumnLabel();
                String ValueLabel = tempMap.getValue();
                Integer timeStampVal = tempMap.getTimeStamp();

                Map tempMap2 = joinMapRows(rowLabel, columnLabel, ValueLabel, timeStampVal);
                tempMap2.print();
                resultantBigT.insertMap(tempMap2.getMapByteArray(), 1);
            } else {
                joinedMaps.add(copyMapObj(tempMap));
            }
            tempMap = tempStream.getNext();
        }
        tempStream.closeStream();

        // Remove duplicates
        getFirstThreeMaps(joinedMaps);

        for (Map tempMap3 : joinedMaps) {
            String rowLabel = leftRowLabel + ":" + rightRowLabel;
            String columnLabel = tempMap3.getColumnLabel();
            String ValueLabel = tempMap3.getValue();
            Integer timeStampVal = tempMap3.getTimeStamp();

            Map tempMap4 = joinMapRows(rowLabel, columnLabel, ValueLabel, timeStampVal);
            tempMap4.print();
            resultantBigT.insertMap(tempMap4.getMapByteArray(), 1);
        }
    }

    public void getFirstThreeMaps(List<Map> commonMapsList) throws Exception {

        for (int i = 0; i < commonMapsList.size(); i++) {
            for (int j = i+1; j < commonMapsList.size(); j++) {
                if( j!=i) {
                    if (commonMapsList.get(i).getValue().equals(commonMapsList.get(j).getValue())) {
                        Map resMap = copyMapObj(commonMapsList.get(i));
                        resMap.setTimeStamp(Math.max(commonMapsList.get(i).getTimeStamp(), commonMapsList.get(j).getTimeStamp()));
                        // setting latest TS as we cannot concatenate int fields
                        commonMapsList.remove(i);
                        commonMapsList.remove(j);
                        commonMapsList.add(resMap);
                    }
                }
            }
        }

        if (commonMapsList.size() > 3) {
            do {
                int tempIdx = 0;
                int minTs = commonMapsList.get(0).getTimeStamp();
                for (int i = 1; i < commonMapsList.size(); i++) {
                    if (commonMapsList.get(i).getTimeStamp() < minTs) {
                        minTs = commonMapsList.get(i).getTimeStamp();
                        tempIdx = i;
                    }
                }

                commonMapsList.remove(tempIdx);
            } while (commonMapsList.size() != 3);
        }
    }

    public Map copyMapObj(Map sourceObj) throws Exception {
        Map destMap = new Map();
        destMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
        destMap.copyMap(sourceObj);
        return destMap;
    }

    public void cleanUp() throws Exception {
        resultantBigT.close();
        this.leftHeapFile.deleteFile();
        this.rightHeapFile.deleteFile();
    }
}
