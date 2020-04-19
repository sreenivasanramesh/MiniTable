package BigT;

import cmdline.MiniTable;
import global.AttrType;
import global.MID;
import global.PageId;
import global.SystemDefs;
import heap.InvalidMapSizeException;
import heap.InvalidTypeException;
import heap.MapScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static global.GlobalConst.MINIBASE_DB_SIZE;
import static global.GlobalConst.NUMBUF;

public class BigTTest {
    
    public static void main(String[] args) throws Exception {
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
         boolean isNewDb = false;
        int numPages = isNewDb ? MINIBASE_DB_SIZE : 0;
        new SystemDefs("/tmp/ash.db", numPages, NUMBUF, "Clock");
        
        bigT bigT;
        if (isNewDb) {
            bigT = new bigT("test1", true);
        } else {
            bigT = new bigT("test1", false);
        }

        BigTTest bigTTest = new BigTTest();
        Map map;
        map = bigTTest.formMap("11", "234", 5, "3");
        bigT.insertMap(map.getMapByteArray(), 2);
        
//        bigT = new bigT("test1", false);
        MapScan mapScan = bigT.heapfiles[2].openMapScan();
        MID mid = new MID();
        map = mapScan.getNext(mid);
        while(map != null){
            map.print();
            map = mapScan.getNext(mid);
        }
    
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
