package BigT;

import btree.*;
import bufmgr.*;
import com.sun.org.apache.xpath.internal.operations.Bool;
import global.*;
import heap.*;
import iterator.MapUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import static global.GlobalConst.*;



//TODO: Make Btree Return MID instead of RID
//TODO: Indexing Types 2, 3, 4, 5 (Working on this)
//TODO: Insert Map (Working on this)
//TODO: Composite Index <value1>$<value2>

public class bigT {
    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    int type;
    String name;
    BTreeFile indexFile;
    BTreeFile timestampIndexFile;
    Heapfile heapfile;
    HashMap<String, ArrayList<MID>> mapVersion;
    
    public bigT(String name) {
        this.name = name;
        try {
            PageId heapFileId = SystemDefs.JavabaseDB.get_file_entry(name + ".meta");
            if (heapFileId == null) {
                throw new Exception("BigT File with name: " + name + " doesn't exist");
            }
            Heapfile metadataFile = new Heapfile(name + ".meta");
            Scan metascan = metadataFile.openScan();
            Tuple metadata = metascan.getNext(new RID());
            metascan.closescan();
            metadata.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            this.type = metadata.getIntFld(1);
            System.out.println("type = " + type);
            this.heapfile = new Heapfile(name + ".heap");
            setIndexFiles();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    public bigT(String name, int type) throws Exception {
        try {
            this.type = type;
            this.name = name;
            Heapfile metadataFile = new Heapfile(name + ".meta");
            Tuple metadata = new Tuple();
            metadata.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            metadata.setIntFld(1, this.type);
            metadataFile.insertRecord(metadata.getTupleByteArray());
            this.heapfile = new Heapfile(name + ".heap");
            this.mapVersion = new HashMap<>();
            createIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void close() throws PageUnpinnedException, PagePinnedException, PageNotFoundException, HashOperationException, BufMgrException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
        if (this.indexFile != null) this.indexFile.close();
        if (this.timestampIndexFile != null) this.timestampIndexFile.close();
        System.out.println("HashMap ->");
        printHashMap();
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Closing BigT File");
    }
    
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
        
        bigT.getRecords();
        Map map = bigT.formMap("1", "2", 3, "4");
        bigT.insertMap(map.getMapByteArray());

        map = bigT.formMap("a", "b", 6, "d");
        bigT.insertMap(map.getMapByteArray());

        map = bigT.formMap("a", "b", 9, "4");
        bigT.insertMap(map.getMapByteArray());
        
        int mapCount = bigT.getMapCnt();
        System.out.println("mapCount = " + mapCount);
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
    
    public void getRecords() throws Exception {
        BTFileScan btFileScan = this.indexFile.new_scan(new StringKey("2"), new StringKey("c"));
        while(true){
            KeyDataEntry kde = btFileScan.get_next();
            if (kde == null){
                break;
            }
            printMap(kde);
        }
        System.out.println("btreefilescan = " + btFileScan.get_next());
    }
    
    //Delete the bigtable from the database.
    void deleteBigt() {
    
    }
    
    // Return number of maps in the bigtable.
    int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        return this.heapfile.getRecCnt();
    }
    
    // Return number of distinct row labels in the bigtable.
    int getRowCnt() {
        return 0;
    }
    
    //    Return number of distinct column labels in the bigtable.
    int getColumnCnt() {
        
        return 0;
    }
    
    private void setIndexFiles() throws Exception {
        switch (this.type) {
            case 1:
                this.indexFile = null;
                break;
            case 2:
                this.indexFile = new BTreeFile(this.name + "_row.idx");
                break;
            case 3:
                this.indexFile = new BTreeFile(this.name + "_col.idx");
                break;
            case 4:
                this.indexFile = new BTreeFile(this.name + "_col_row.idx");
                this.timestampIndexFile = new BTreeFile(this.name + "_timestamp.idx");
                break;
            case 5:
                this.indexFile = new BTreeFile(this.name + "row_val.idx");
                this.timestampIndexFile = new BTreeFile(this.name + "_timestamp.idx");
                break;
            default:
                throw new Exception("Invalid Index Type");
        }
    }
    
    private void createIndex() throws Exception {
        switch (this.type) {
            case 1:
                this.indexFile = null;
                break;
            case 2:
                this.indexFile = new BTreeFile(this.name + "_row.idx", AttrType.attrString, 20, DeleteFashion.NAIVE_DELETE);
                break;
            case 3:
                this.indexFile = new BTreeFile(this.name + "_col.idx", AttrType.attrString, 20, DeleteFashion.NAIVE_DELETE);
                break;
            case 4:
                this.indexFile = new BTreeFile(this.name + "_col_row.idx", AttrType.attrString, 20, DeleteFashion.NAIVE_DELETE);
                this.timestampIndexFile = new BTreeFile(this.name + "_timestamp.idx", AttrType.attrInteger, 4, DeleteFashion.NAIVE_DELETE);
                break;
            case 5:
                this.indexFile = new BTreeFile(this.name + "row_val.idx", AttrType.attrString, 20, DeleteFashion.NAIVE_DELETE);
                this.timestampIndexFile = new BTreeFile(this.name + "_timestamp.idx", AttrType.attrInteger, 4, DeleteFashion.NAIVE_DELETE);
                break;
            default:
                throw new Exception("Invalid Index Type");
        }
    }
    
    private void printMap(KeyDataEntry keyDataEntry) throws Exception {
        LeafData dataClass = (LeafData) keyDataEntry.data;
        RID rra = dataClass.getData();
        MID midi = MapUtils.midFromRid(rra);
        Map mappa = this.heapfile.getMap(midi);
        mappa.print();
    }
    
    private void printHashMap(){
        mapVersion.entrySet().forEach(entry->{ System.out.println(entry.getKey() + ":" + entry.getValue().toString());});
    }
    
    
    // TODO: insert and return MID
    // This has to be modified to take care of storing 3 versions of a map at any point in time
    MID insertMap(byte[] mapPtr) throws Exception {
        MID mid = this.heapfile.insertMap(mapPtr);
        RID rid = MapUtils.ridFromMid(mid);
        String key;
        Map map = new Map();
        map.setData(mapPtr);
        switch (this.type) {
            case 1:
                key = null;
                break;
            case 2:
                key = map.getRowLabel();
                break;
            case 3:
                key = map.getColumnLabel();
                break;
            case 4:
                key = map.getColumnLabel() + "$" + map.getRowLabel();
                this.timestampIndexFile.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
            case 5:
                key = map.getRowLabel() + "$" + map.getValue();
                this.timestampIndexFile.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
            default:
                throw new Exception("Invalid Index Type");
        }
        if (key != null) {
            System.out.println("key = " + key);
            this.indexFile.insert(new StringKey(key), rid);
        }
    
        String mapVersionKey = map.getRowLabel() + "$" + map.getColumnLabel();
        ArrayList<MID> list = mapVersion.get(mapVersionKey);
        if (list == null){
            list = new ArrayList<>();
        }
        list.add(mid);
        mapVersion.put(mapVersionKey, list);
        return mid;
    }
    
    public Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.
            lang.String valueFilter) {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
}