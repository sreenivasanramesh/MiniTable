package BigT;

import btree.*;
import bufmgr.*;
import cmdline.MiniTable;
import global.*;
import heap.*;
import iterator.MapUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static global.GlobalConst.MINIBASE_PAGESIZE;


public class bigT {
    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    
    // Indexing type
    int type;
    
    // Name of the BigT file
    String name;
    
    // Btree Index file: row, col, col-row, row-value
    BTreeFile indexFile;
    
    // Btree Index file on timestamp when index type is 4 or 5
    BTreeFile timestampIndexFile;
    
    // Heap file which stores maps
    Heapfile heapfile;
    
    public int getType() {
        return type;
    }
    
    public void setType(int type) {
        this.type = type;
    }
    
    // HashMap used for maintaining map versions
    HashMap<String, ArrayList<MID>> mapVersion;
    
    // Open an existing BigT file
    public bigT(String name) {
        
        this.name = name;
        try {
            PageId heapFileId = SystemDefs.JavabaseDB.get_file_entry(name + ".meta");
            if (heapFileId == null) {
                throw new Exception("BigT File with name: " + name + " doesn't exist");
            }
            
            // Load the metadata from .meta heapfile
            Heapfile metadataFile = new Heapfile(name + ".meta");
            Scan metascan = metadataFile.openScan();
            Tuple metadata = metascan.getNext(new RID());
            metadata.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            metascan.closescan();
            this.type = metadata.getIntFld(1);
            
            // Set the Indexfile names from the type
            setIndexFiles();
            
            // Open the Heap file which is used for storing the maps
            this.heapfile = new Heapfile(name + ".heap");
            
            // Load the mapVersion HashMap from the disk
            try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("/tmp/" + this.name + ".hashmap.ser"))) {
                this.type = objectInputStream.readByte();
                this.mapVersion = (HashMap<String, ArrayList<MID>>) objectInputStream.readObject();
            } catch (IOException e) {
                throw new IOException("File not writable: " + e.toString());
            }
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    // Create a new BigT file
    public bigT(String name, int type) throws Exception {
        try {
            this.type = type;
            this.name = name;
            
            // Create a new heap file name + .meta for storing the metadata of the table
            Heapfile metadataFile = new Heapfile(name + ".meta");
            Tuple metadata = new Tuple();
            metadata.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            metadata.setIntFld(1, this.type);
            metadataFile.insertRecord(metadata.getTupleByteArray());
            
            // Create the heap file for storing the Maps
            this.heapfile = new Heapfile(name + ".heap");
            
            // Initialize the HashMap used for maintaining versions
            this.mapVersion = new HashMap<>();
            
            //
            createIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void close() throws PageUnpinnedException, PagePinnedException, PageNotFoundException, HashOperationException, BufMgrException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
        if (this.indexFile != null) this.indexFile.close();
        if (this.timestampIndexFile != null) this.timestampIndexFile.close();
        
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("/tmp/" + this.name + ".hashmap.ser"))) {
            objectOutputStream.writeByte(type);
            objectOutputStream.writeObject(mapVersion);
        } catch (IOException e) {
            throw new IOException("File not writable: " + e.toString());
        }
    }
    
    
    // This method is temporary. Should Ideally use Stream class to get the records
    public void getRecords() throws Exception {
        BTFileScan btFileScan = this.indexFile.new_scan(new StringKey("New_Jersey"), new StringKey("New_JerseyZ"));
//        BTFileScan btFileScan = this.indexFile.new_scan(null, null);
        while (true) {
            KeyDataEntry kde = btFileScan.get_next();
            if (kde == null) {
                System.out.println("Map is null");
                break;
            }
            printMap(kde);
        }
    }
    

    private void printMap(KeyDataEntry keyDataEntry) throws Exception {
        LeafData dataClass = (LeafData) keyDataEntry.data;
        RID rra = dataClass.getData();
        MID midi = MapUtils.midFromRid(rra);
        Map mappa = this.heapfile.getMap(midi);
        mappa.print();
    }


    // Return number of maps in the bigtable.
    public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        return this.heapfile.getRecCnt();
    }
    
    // Return number of distinct row labels in the bigtable.
    public int getRowCnt() {
        Set<String> distinctRow = new HashSet<>();
        mapVersion.keySet().forEach(key -> distinctRow.add(key.split("\\$")[0]));
        return distinctRow.size();
    }
    
    // Return number of distinct column labels in the bigtable.
    public int getColumnCnt() {
        Set<String> distinctCol = new HashSet<>();
        mapVersion.keySet().forEach(key -> distinctCol.add(key.split("\\$")[1]));
        return distinctCol.size();
    }
    
    // Return number of distinct ts labels in the bigtable.
    int getTimeStampCnt() {
        Set<String> distinctTS = new HashSet<>();
        mapVersion.keySet().forEach(key -> distinctTS.add(key.split("\\$")[3]));
        return distinctTS.size();
    }
    
    // Opens the Btree index files based on type and stores it in instance variable indexFile and timestampIndex file
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
    
    
    // Creates the required btree index filed based on type and stores it in the instance variable indexFile and timestampIndex file
    private void createIndex() throws Exception {
        switch (this.type) {
            case 1:
                this.indexFile = null;
                break;
            case 2:
                this.indexFile = new BTreeFile(this.name + "_row.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0], DeleteFashion.NAIVE_DELETE);
                break;
            case 3:
                this.indexFile = new BTreeFile(this.name + "_col.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[1], DeleteFashion.NAIVE_DELETE);
                break;
            case 4:
                this.indexFile = new BTreeFile(this.name + "_col_row.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[1] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE);
                this.timestampIndexFile = new BTreeFile(this.name + "_timestamp.idx", AttrType.attrInteger, 4, DeleteFashion.NAIVE_DELETE);
                break;
            case 5:
                this.indexFile = new BTreeFile(this.name + "row_val.idx", AttrType.attrString, MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[2] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE);
                this.timestampIndexFile = new BTreeFile(this.name + "_timestamp.idx", AttrType.attrInteger, 4, DeleteFashion.NAIVE_DELETE);
                break;
            default:
                throw new Exception("Invalid Index Type");
        }
    }

    
    // This has to be modified to take care of storing 3 versions of a map at any point in time
    public MID insertMap(byte[] mapPtr) throws Exception {
        Map map = new Map();
        map.setData(mapPtr);
        
        String key;
        String mapVersionKey = map.getRowLabel() + "$" + map.getColumnLabel();
        ArrayList<MID> list = mapVersion.get(mapVersionKey);
        if (list == null) {
            list = new ArrayList<>();
        } else {
            int oldestTimestamp = Integer.MAX_VALUE;
            MID oldestMID = null;
            Map oldestMap = new Map();
            if (list.size() > 3) {
                throw new IOException("Metadata file is corrupted, please delete it");
            }
            if (list.size() == 3) {
                for (MID mid1 : list) {
                    Map map1 = heapfile.getMap(mid1);
                    if (MapUtils.Equal(map1, map)) {
                        return mid1;
                    } else {
                        if (map1.getTimeStamp() < oldestTimestamp) {
                            oldestTimestamp = map1.getTimeStamp();
                            oldestMID = mid1;
                            oldestMap = map1;
                        }
                    }
                }
            }
            if (list.size() == 3 && map.getTimeStamp() < oldestTimestamp) {
                return oldestMID;
            }
            
            if (list.size() == 3) {
//                Map oldestMap = heapfile.getMap(oldestMID);
                switch (this.type) {
                    case 1:
                        key = null;
                        break;
                    case 2:
                        key = oldestMap.getRowLabel();
                        break;
                    case 3:
                        key = oldestMap.getColumnLabel();
                        break;
                    case 4:
                        key = oldestMap.getColumnLabel() + "$" + oldestMap.getRowLabel();
                        this.timestampIndexFile.Delete(new IntegerKey(oldestMap.getTimeStamp()), MapUtils.ridFromMid(oldestMID));
                        break;
                    case 5:
                        key = oldestMap.getRowLabel() + "$" + oldestMap.getValue();
                        this.timestampIndexFile.Delete(new IntegerKey(oldestMap.getTimeStamp()), MapUtils.ridFromMid(oldestMID));
                        break;
                    default:
                        throw new Exception("Invalid Index Type");
                }
                if (key != null) {
                    this.indexFile.Delete(new StringKey(key), MapUtils.ridFromMid(oldestMID));
                }
                heapfile.deleteMap(oldestMID);
                list.remove(oldestMID);
                
            }
        }
        MID mid = this.heapfile.insertMap(mapPtr);
        RID rid = MapUtils.ridFromMid(mid);
        list.add(mid);
        mapVersion.put(mapVersionKey, list);
        
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
            this.indexFile.insert(new StringKey(key), rid);
        }
        return mid;
    }


    public Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.
            lang.String valueFilter) throws Exception {
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }
    
}