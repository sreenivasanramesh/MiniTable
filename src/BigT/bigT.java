package BigT;

import btree.BTreeFile;
import btree.DeleteFashion;
import btree.StringKey;
import bufmgr.*;
import cmdline.MiniTable;
import cmdline.Utils;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static global.GlobalConst.MINIBASE_PAGESIZE;


public class bigT {
    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    public Heapfile[] heapfiles;
    String[] heapfileNames;
    String[] indexfileNames;
    // Name of the BigT file
    String name;
    BTreeFile[] indexFiles;
    
    public bigT(String name, boolean createNew) {
        
        this.name = name;
        try {
            Boolean tableExists;
            this.heapfileNames = new String[]{name + ".no.heap", name + ".row.heap", name + ".col.heap", name + ".col_row.heap", name + ".row_val.heap"};
            this.indexfileNames = new String[]{null, name + ".row.idx", name + ".col.idx", name + ".col_row.idx", name + ".row_val.idx"};
            PageId heapFilePageId = SystemDefs.JavabaseDB.get_file_entry(this.heapfileNames[0]);
    
            tableExists = heapFilePageId != null;
    
    
            if (!tableExists) {
                Utils.addTableToInventory(name);
                this.indexFiles = new BTreeFile[]{null, new BTreeFile(name + ".row.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0], DeleteFashion.NAIVE_DELETE),
                        new BTreeFile(name + ".col.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[1], DeleteFashion.NAIVE_DELETE),
                        new BTreeFile(name + ".col_row.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[1] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE),
                        new BTreeFile(name + ".row_val.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[2] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE)};
            } else {
                this.indexFiles = new BTreeFile[]{null, new BTreeFile(name + ".row.idx"), new BTreeFile(name + ".col.idx"), new BTreeFile(name + ".col_row.idx"), new BTreeFile(name + ".row_val.idx")};
            }
    
            this.heapfiles = new Heapfile[]{new Heapfile(name + ".no.heap"), new Heapfile(name + ".row.heap"), new Heapfile(name + ".col.heap"), new Heapfile(name + ".col_row.heap"), new Heapfile(name + ".row_val.heap")};
    
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    public void close() throws PageUnpinnedException, PagePinnedException, PageNotFoundException, HashOperationException, BufMgrException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
        for (int i = 0; i < 5; i++) {
            if (this.indexFiles[i] != null) {
                this.indexFiles[i].close();
            }
        }
    }
    
    public void batchInsert(Heapfile heapfile, int type) throws Exception {
        Set<Integer> deletedTypes = new HashSet<>();
        type -= 1;
        MID oldestMID = null;
        
        
        MapScan mapScan = heapfile.openMapScan();
        int heapfileCount = heapfile.getRecCnt();
        MID mid = new MID();
        Map map = mapScan.getNext(mid);
        int count = 1;
        while (map != null) {
            int oldestTimestamp = Integer.MAX_VALUE;
            int oldestType = -1;
            int updateType = -1;
            MID updateMID = null;
            System.out.print("\r" + count + " / " + heapfileCount);
            count += 1;
            java.util.Map<Integer, ArrayList<MID>> searchResults = searchForRecords(map);
            ArrayList<MID> arrayList = new ArrayList<>();
            searchResults.values().forEach(arrayList::addAll);
    
            if (arrayList.size() > 3) {
                throw new Exception("This list size cannot be greater than 3");
            }
            for (Integer key : searchResults.keySet()) {
                for (MID mid1 : searchResults.get(key)) {
                    Map map1 = this.heapfiles[key].getMap(mid1);
                    if (arrayList.size() == 3) {
                        if (map1.getTimeStamp() < oldestTimestamp) {
                            oldestMID = mid1;
                            oldestTimestamp = map1.getTimeStamp();
                            oldestType = key;
                        }
                    }
                    if (map1.getTimeStamp() == map.getTimeStamp()) {
                        updateMID = mid1;
                        updateType = key;
                    }
                }
            }
            if (oldestType != -1) {
                if (map.getTimeStamp() < oldestTimestamp) {
                    map = mapScan.getNext(mid);
                    continue;
                }
            }
            if (updateMID != null) {
                this.heapfiles[updateType].deleteMap(updateMID);
            } else {
                if (oldestType != -1) {
                    this.heapfiles[oldestType].deleteMap(oldestMID);
                    deletedTypes.add(oldestType);
                }
            }
            this.heapfiles[type].insertMap(map.getMapByteArray());
            map = mapScan.getNext(mid);
        }
        System.out.println("");
        deletedTypes.add(type);
        for (int i : deletedTypes) {
            if (i != 0) {
                insertMapFile(i);
            }
        }
        mapScan.closescan();
    }
    
    // Return number of maps in the bigtable.
    public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        int count = 0;
        for (int i = 0; i < 5; i++) {
            count += this.heapfiles[i].getRecCnt();
        }
        return count;
    }
    
    // Return number of distinct row labels in the bigtable.
    public int getRowCnt() throws Exception {
        MiniTable.orderType = 1;
        Stream stream = this.openStream(1, "*", "*", "*");
        Map map = stream.getNext();
        String oldRowKey = map.getRowLabel();
        int distinctRows = 0;
        while (map != null) {
            if (!map.getRowLabel().equals(oldRowKey)) distinctRows += 1;
            oldRowKey = map.getRowLabel();
            map = stream.getNext();
        }
        distinctRows += 1;
        stream.closeStream();
        return distinctRows;
    }
    
    // Return number of distinct column labels in the bigtable.
    public int getColumnCnt() throws Exception {
        MiniTable.orderType = 2;
        Stream stream = this.openStream(2, "*", "*", "*");
        Map map = stream.getNext();
        String oldColKey = map.getColumnLabel();
        int distinctCols = 0;
        while (map != null) {
            if (!map.getColumnLabel().equals(oldColKey)) distinctCols += 1;
            oldColKey = map.getColumnLabel();
            map = stream.getNext();
        }
        distinctCols += 1;
        stream.closeStream();
        return distinctCols;
    }
    
    public void insertMap(byte[] mapPtr, int type) throws Exception {
        type -= 1;
        MID oldestMID = null;
        MID updateMID = null;
        int oldestType = -1;
        int updateType = -1;
        int oldestTimestamp = Integer.MAX_VALUE;
        Map map = new Map();
        map.setData(mapPtr);
        java.util.Map<Integer, ArrayList<MID>> searchResults = searchForRecords(map);
        ArrayList<MID> arrayList = new ArrayList<>();
        searchResults.values().forEach(arrayList::addAll);
        if (arrayList.size() > 3) {
            throw new Exception("This list size cannot be greater than 3");
        }
        for (Integer key : searchResults.keySet()) {
            for (MID mid1 : searchResults.get(key)) {
                Map map1 = this.heapfiles[key].getMap(mid1);
                if (arrayList.size() == 3) {
                    if (map1.getTimeStamp() < oldestTimestamp) {
                        oldestMID = mid1;
                        oldestTimestamp = map1.getTimeStamp();
                        oldestType = key;
                    }
                }
                if (map1.getTimeStamp() == map.getTimeStamp()) {
                    updateMID = mid1;
                    updateType = key;
                }
            }
        }
        if (oldestType != -1) {
            if (map.getTimeStamp() < oldestTimestamp) {
                return;
            }
        }
        if (updateType != -1) {
            this.heapfiles[updateType].deleteMap(updateMID);
        } else {
            if (oldestType != -1) {
                this.heapfiles[oldestType].deleteMap(oldestMID);
            }
        }
    
        if (oldestType != -1 && oldestType != 0) {
            insertMapFile(oldestType);
        }
        this.heapfiles[type].insertMap(mapPtr);
        if (type != 0) {
            insertMapFile(type);
        }
    
    }
    
    private void insertMapFile(int type) throws HFDiskMgrException, InvalidTupleSizeException, InvalidMapSizeException, IOException, InvalidSlotNumberException, SpaceNotAvailableException, HFException, HFBufMgrException {
        MiniTable.insertType = type;
        MID mid = new MID();
        MapScan mapScan = this.heapfiles[type].openMapScan();
        Heapfile tempHeapFile = new Heapfile(String.format("%s.%d.tmp.heap", this.name, type));
        Map map1 = mapScan.getNext(mid);
        while (map1 != null) {
            tempHeapFile.insertMap(map1.getMapByteArray());
            map1 = mapScan.getNext(mid);
        }
        mapScan.closescan();
        FileScan fscan = null;
        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);
        
        try {
            fscan = new FileScan(String.format("%s.%d.tmp.heap", this.name, type), MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int sortField, num_pages = 10, sortFieldLength;
        MapSort sortObj;
        switch (type) {
            case 1:
            case 4:
                sortField = 1;
                sortFieldLength = MiniTable.BIGT_STR_SIZES[0];
                break;
            case 2:
            case 3:
                sortField = 2;
                sortFieldLength = MiniTable.BIGT_STR_SIZES[1];
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        try {
            this.heapfiles[type].deleteFile();
            this.indexFiles[type].destroyFile();
            switch (type) {
                case 1:
                    this.indexFiles[type] = new BTreeFile(name + ".row.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0], DeleteFashion.NAIVE_DELETE);
                    break;
                case 2:
                    this.indexFiles[type] = new BTreeFile(name + ".col.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[1], DeleteFashion.NAIVE_DELETE);
                    break;
                case 3:
                    this.indexFiles[type] = new BTreeFile(name + ".col_row.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[1] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE);
                    break;
                case 4:
                    this.indexFiles[type] = new BTreeFile(name + ".row_val.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[2] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE);
                    break;
                default:
                    throw new Exception("Undefined value");
            }
            this.heapfiles[type] = new Heapfile(this.heapfileNames[type]);
            sortObj = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, fscan, sortField, new TupleOrder(TupleOrder.Ascending), num_pages, sortFieldLength, true);
            Map map2 = sortObj.get_next();
            while (map2 != null) {
                MID mid1 = this.heapfiles[type].insertMap(map2.getMapByteArray());
                StringKey stringKey;
                switch (type) {
                    case 1:
                        stringKey = new StringKey(map2.getRowLabel());
                        break;
                    case 2:
                        stringKey = new StringKey(map2.getColumnLabel());
                        break;
                    case 3:
                        stringKey = new StringKey(map2.getColumnLabel() + "$" + map2.getRowLabel());
                        break;
                    case 4:
                        stringKey = new StringKey(map2.getRowLabel() + "$" + map2.getValue());
                        break;
                    default:
                        throw new Exception("undefined value");
                }
                this.indexFiles[type].insert(stringKey, MapUtils.ridFromMid(mid1));
                map2 = sortObj.get_next();
            }
            assert fscan != null;
            fscan.close();
            sortObj.close();
            tempHeapFile.deleteFile();
    
    
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void addToArrayList(Map newMap, Map oldMap, java.util.Map<Integer, ArrayList<MID>> searchResults, MID mid, int key) throws IOException {
        if (MapUtils.checkSameMap(newMap, oldMap)) {
            MID tempMid = new MID();
            tempMid.setSlotNo(mid.getSlotNo());
            tempMid.setPageNo(mid.getPageNo());
            ArrayList<MID> arrayList = searchResults.get(key) == null ? new ArrayList<>() : searchResults.get(key);
            arrayList.add(tempMid);
            searchResults.put(key, arrayList);
        }
    }
    
    private java.util.Map<Integer, ArrayList<MID>> searchForRecords(Map newMap) throws Exception {
        java.util.Map<Integer, ArrayList<MID>> searchResults = new HashMap<>();
        
        for (int i = 0; i < 5; i++) {
            MapScan mapScan = this.heapfiles[i].openMapScan();
            MID mid = new MID();
            Map map = mapScan.getNext(mid);
            
            while (map != null) {
                addToArrayList(newMap, map, searchResults, mid, i);
                map = mapScan.getNext(mid);
            }
            mapScan.closescan();
        }

//        for (short i = 1; i < 5; i++) {
//            StringKey stringKey;
//            switch (i) {
//                case 1:
//                    stringKey = new StringKey(newMap.getRowLabel());
//                    break;
//                case 2:
//                    stringKey = new StringKey(newMap.getColumnLabel());
//                    break;
//                case 3:
//                    stringKey = new StringKey(newMap.getColumnLabel() + "$" + newMap.getRowLabel());
//                    break;
//                case 4:
//                    stringKey = new StringKey(newMap.getRowLabel() + "$" + newMap.getValue());
//                    break;
//                default:
//                    throw new Exception("Invalid Case");
//            }
//            BTFileScan btFileScan = this.indexFiles[i].new_scan(stringKey, new StringKey(stringKey.getKey() + "a"));
//            KeyDataEntry keyDataEntry = btFileScan.get_next();
//            while (keyDataEntry != null) {
//                RID rid = ((LeafData) keyDataEntry.data).getData();
//                if (rid != null) {
//                    MID midFromRid = MapUtils.midFromRid(rid);
//                    map = this.heapfiles[i].getMap(midFromRid);
//                    addToArrayList(newMap, map, searchResults, midFromRid, i);
//                }
//                keyDataEntry = btFileScan.get_next();
//            }
//        }
        return searchResults;
    }
    
    public Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.
            lang.String valueFilter) throws Exception {
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }
    
}