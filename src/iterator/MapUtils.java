package iterator;

import BigT.Map;
import cmdline.MiniTable;
import global.AttrType;
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

    public static void SetValue(Map m1, Map m2, int map_fld_no, AttrType fldType)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException {
        String m1_s, m2_s;
        int m1_i, m2_i;
        switch (map_fld_no) {
            case 1:
                m1.setRowLabel(m2.getRowLabel());
                break;
            case 2:
                m1.setColumnLabel(m2.getColumnLabel());
            case 3:
                m1.setTimeStamp(m2.getTimeStamp());
            case 4:
                m1.setValue(m2.getValue());
        }
    }

    public static int CompareMapWithValue( Map m1, int fieldNo, String value)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException, InvalidFieldNo {
        Map m2 = new Map();
        switch (fieldNo) {
            case 1:
                m2.setRowLabel(value);
            case 2:
                m2.setColumnLabel(value);
            case 3:
                m2.setValue(value);
        }
        return CompareMapWithMap(m1, m2, fieldNo);
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

    public static MID midFromRid(RID rid) {
        MID mid = new MID();
        mid.setPageNo(rid.pageNo);
        mid.setSlotNo(rid.slotNo);
        return mid;
    }


    public static short[] setup_op_tuple(Map Jmap, AttrType[] res_attrs,
                                         AttrType[] in1, int len_in1,
                                         short[] t1_str_sizes,
                                         FldSpec[] proj_list, int nOutFlds)
            throws IOException,
            TupleUtilsException,
            InvalidRelation {
        short[] sizesT1 = new short[len_in1];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);

            else throw new InvalidRelation("Invalid relation -innerRel");
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short[n_strs];
        count = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
        }

        try {
            Jmap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "setHdr() failed");
        }
        return res_str_sizes;
    }

}