package iterator;

import BigT.Map;
import global.MID;
import global.RID;

import java.io.IOException;

public class MapUtils {
    public static int CompareMapWithMap(Map map1, Map map2, int fieldNo)
            throws IOException,
            InvalidFieldNo {
        int map1Int, map2Int;
        String map1Str, map2Str;
        
        switch (fieldNo) {
            case 0:
                map1Str = map1.getRowLabel();
                map2Str = map2.getRowLabel();
                return Integer.compare(map1Str.compareTo(map2Str), 0);
            case 1:
                map1Str = map1.getColumnLabel();
                map2Str = map2.getColumnLabel();
                return Integer.compare(map1Str.compareTo(map2Str), 0);
            case 2:
                map1Int = map1.getTimeStamp();
                map2Int = map2.getTimeStamp();
                return Integer.compare(map1Int, map2Int);
            case 3:
                map1Str = map1.getValue();
                map2Str = map2.getValue();
                return Integer.compare(map1Str.compareTo(map2Str), 0);
            default:
                throw new InvalidFieldNo("Field Number should be in the range (0,3)");
        }
    }
    
    public static boolean Equal(Map map1, Map map2)
            throws IOException,
            InvalidFieldNo {
        
        for (int i = 0; i <= 3; i++) {
            if (CompareMapWithMap(map1, map2, i) != 0)
                return false;
        }
        return true;
    }
    
    public static RID ridFromMid(MID mid){
        return new RID(mid.getPageNo(), mid.getSlotNo());
    }
    
    public static MID midFromRid(RID rid){
        MID mid = new MID();
        mid.setPageNo(rid.pageNo);
        mid.setSlotNo(rid.slotNo);
        return mid;
    }
}