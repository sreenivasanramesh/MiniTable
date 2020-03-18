package BigT;

import global.AttrType;
import global.SystemDefs;
import heap.InvalidMapSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

import static global.GlobalConst.MINIBASE_DB_SIZE;
import static global.GlobalConst.NUMBUF;

public class BigTTest {
    
    public static void main(String[] args) throws Exception {
        boolean isNewDb = true;
        int numPages = isNewDb ? MINIBASE_DB_SIZE : 0;
        new SystemDefs("/Users/sumukhashwinkamath/test.db", numPages, NUMBUF, "Clock");
        bigT bigT;
        if (isNewDb) {
            bigT = new bigT("test1", 3);
        } else {
            bigT = new bigT("test1");
        }
        
        BigTTest bigTTest = new BigTTest();
        Map map = bigTTest.formMap("1", "2", 3, "4");
        bigT.insertMap(map.getMapByteArray());
        
        map = bigTTest.formMap("a", "b", 6, "d");
        bigT.insertMap(map.getMapByteArray());
        
        map = bigTTest.formMap("a", "c", 9, "4");
        bigT.insertMap(map.getMapByteArray());
        
        map = bigTTest.formMap("a", "d", 10, "6");
        bigT.insertMap(map.getMapByteArray());
        
        int rowCnt = bigT.getRowCnt();
        System.out.println("rowCnt = " + rowCnt);
        
        int colCnt = bigT.getColumnCnt();
        System.out.println("colCnt = " + colCnt);
        
        int mapCount = bigT.getMapCnt();
        System.out.println("mapCount = " + mapCount);
        
        bigT.getRecords();
        bigT.close();
    }
    
    private Map formMap(String row, String col, int timestamp, String value) throws IOException, InvalidMapSizeException, InvalidStringSizeArrayException, InvalidTypeException {
        Map map = new Map();
        short[] strSizes = new short[]{(short) row.getBytes().length, (short) col.getBytes().length, (short) value.getBytes().length};
        map.setHeader(new AttrType[]{new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)}, strSizes);
        map.setRowLabel(row);
        map.setColumnLabel(col);
        map.setTimeStamp(timestamp);
        map.setValue(value);
        return map;
    }
}
