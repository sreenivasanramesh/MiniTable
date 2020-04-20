package BigT;

import cmdline.MiniTable;
import global.AttrType;
import global.MID;
import global.PageId;
import global.SystemDefs;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static global.GlobalConst.MINIBASE_DB_SIZE;
import static global.GlobalConst.NUMBUF;

public class BigTTest {
    
    public void testMID(MID mid){
        mid.setPageNo(new PageId(10));
        mid.setSlotNo(20);
    }
    public void addArrayList(java.util.Map<Integer, ArrayList<MID>> searchResults, MID mid){
        Integer key = 1;
        ArrayList<MID> arrayList = searchResults.get(key) == null ? new ArrayList<>() : searchResults.get(key);
        System.out.println(arrayList);
        arrayList.add(mid);
        System.out.println(arrayList);
        searchResults.put(key, arrayList);
    }
    
    public void testHashMid(){
        java.util.Map<Integer, ArrayList<MID>> searchResults = new HashMap<>();
        Integer key = 1;
        MID mid = new MID();
        mid.setSlotNo(10);
        mid.setPageNo(new PageId(20));
        addArrayList(searchResults, mid);
        
        mid.setSlotNo(20);
        mid.setPageNo(new PageId(30));
        addArrayList(searchResults, mid);
        
        mid.setSlotNo(30);
        mid.setPageNo(new PageId(40));
        addArrayList(searchResults, mid);
        
        System.out.println(searchResults);
    }
    
    public void checkHeap() throws Exception {
        Heapfile hf = new Heapfile("testtttt");
        Map map = new Map();
        map.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
        map.setColumnLabel("2");
        map.setRowLabel("1");
        map.setTimeStamp(1);
        map.setValue("45");
        MID mid = hf.insertMap(map.getMapByteArray());
        MID mid1 = new MID();
        mid1.setSlotNo(mid.getSlotNo());
        mid1.setPageNo(mid.getPageNo());
        Map map2 = hf.getMap(mid);
        System.out.println("Check");
        map2.print();
    }
    
    public void printHeapFiles(bigT bigT) throws InvalidTupleSizeException, IOException {
        Map map;
        System.out.println("=========================");
        for(int i=0;i<5;i++){
            System.out.println("=====Heap file " + i);
            MapScan mapScan = bigT.heapfiles[i].openMapScan();
            MID mid = new MID();
            map = mapScan.getNext(mid);
            while(map != null){
                map.print();
                map = mapScan.getNext(mid);
            }
        }
        System.out.println("=========================");
    }
    
    public void printSameLine(){
        
        for(int i=0; i<Integer.MAX_VALUE;i++){
            System.out.print("\r" + i);
        }
    }
    
    public static void main(String[] args) throws Exception {
        boolean isNewDb = true;
        int numPages = isNewDb ? MINIBASE_DB_SIZE : 0;
        System.out.println("numPages = " + numPages);
        new SystemDefs("/tmp/ash_test.db", numPages, NUMBUF, "Clock");
        
        MID midtest = new MID();
        BigTTest bigTTest = new BigTTest();
        bigTTest.checkHeap();
        int test = 0;
        if (test == 0){
            return;
        }
        bigTTest.testMID(midtest);
        System.out.println("midtest = " + midtest);
        
        bigTTest.printSameLine();
       
//        java.util.Map<Integer, ArrayList<Integer>> test = new HashMap<>();
//        System.out.println(test.get(0));
        
//        BigTTest bigTTest = new BigTTest();
//        MID mid = new MID();
//        bigTTest.checkMid(mid);
//        System.out.println("mid.getPageNo() = " + mid.getPageNo().pid);
//        System.out.println("mid.getSlotNo() = " + mid.getSlotNo());
//
//        final long startTime = System.currentTimeMillis();
//
        
        bigT bigT;
        if (isNewDb) {
            bigT = new bigT("test1", true);
        } else {
            bigT = new bigT("test1", false);
        }
        
        Map map;
        map = bigTTest.formMap("10", "23", 10, "6");
        bigT.insertMap(map.getMapByteArray(), 2);
//        bigTTest.printHeapFiles(bigT);
        map = bigTTest.formMap("10", "23", 15, "9");
        bigT.insertMap(map.getMapByteArray(), 3);
//        bigTTest.printHeapFiles(bigT);
        map = bigTTest.formMap("10", "23", 20, "12");
        bigT.insertMap(map.getMapByteArray(), 2);
//        bigTTest.printHeapFiles(bigT);
        map = bigTTest.formMap("10", "23", 25, "200");
        bigT.insertMap(map.getMapByteArray(), 4);
//        bigTTest.printHeapFiles(bigT);
        map = bigTTest.formMap("10", "23", 27, "400");
        bigT.insertMap(map.getMapByteArray(), 2);
//        bigTTest.printHeapFiles(bigT);
        map = bigTTest.formMap("10", "23", 30, "60");
        bigT.insertMap(map.getMapByteArray(), 2);
        bigTTest.printHeapFiles(bigT);
        
//        bigT = new bigT("test1", false);
        
        
    
        bigT.close();
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        
        
//
//        map = bigTTest.formMap("1", "2", 4, "4");
//        bigT.insertMap(map.getMapByteArray());
//
//        map = bigTTest.formMap("1", "2", 5, "4");
//        bigT.insertMap(map.getMapByteArray());
//
//        map = bigTTest.formMap("1", "2", 6, "4");
//        bigT.insertMap(map.getMapByteArray());
//
//        map = bigTTest.formMap("a", "b", 6, "d");
//        bigT.insertMap(map.getMapByteArray());
//
//        map = bigTTest.formMap("a", "c", 9, "4");
//        bigT.insertMap(map.getMapByteArray());
//
//        map = bigTTest.formMap("a", "d", 10, "6");
//        bigT.insertMap(map.getMapByteArray());
//        bigT.printFullScan();
//
//        bigT.getRecords();
//        int rowCnt = bigT.getRowCnt();
//        System.out.println("rowCnt = " + rowCnt);
//
//        int colCnt = bigT.getColumnCnt();
//        System.out.println("colCnt = " + colCnt);
//
//        int mapCount = bigT.getMapCnt();
//        System.out.println("mapCount = " + mapCount);
//
//        bigT.close();
    
    }
    
    private Map formMap(String row, String col, int timestamp, String value) throws IOException, InvalidMapSizeException, InvalidStringSizeArrayException, InvalidTypeException {
        Map map = new Map();
        short[] strSizes = new short[]{(short) row.getBytes().length, (short) col.getBytes().length, (short) value.getBytes().length};
        map.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
        map.setRowLabel(row);
        map.setColumnLabel(col);
        map.setTimeStamp(timestamp);
        map.setValue(value);
        return map;
    }
}
