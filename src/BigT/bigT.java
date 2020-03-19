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
import java.util.HashSet;
import java.util.Set;

import static global.GlobalConst.*;



//TODO: Make Btree Return MID instead of RID
//TODO: Indexing Types 2, 3, 4, 5 (Working on this)
//TODO: Insert Map (Working on this)
//TODO: Composite Index <value1>$<value2>

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
            System.out.println("type = " + type);

            // Set the Indexfile names from the type
            setIndexFiles();

            // Open the Heap file which is used for storing the maps
            this.heapfile = new Heapfile(name + ".heap");

            // Load the mapVersion HashMap from the disk


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
        System.out.println("HashMap ->");
        printMapVersion();

        // Add logic to store the HashMap to Metadata heap file


        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Successfully closed the BigT File");
    }


    // This method is temporary. Should Ideally use Stream class to get the records
    public void getRecords() throws Exception {
        BTFileScan btFileScan = this.indexFile.new_scan(new StringKey("2"), new StringKey("c"));
        while(true){
            KeyDataEntry kde = btFileScan.get_next();
            if (kde == null){
                break;
            }
            printMap(kde);
        }
    }

    // This should ideally be removed. This functionality should be a part of Stream class
    private void printMap(KeyDataEntry keyDataEntry) throws Exception {
        LeafData dataClass = (LeafData) keyDataEntry.data;
        RID rra = dataClass.getData();
        MID midi = MapUtils.midFromRid(rra);
        Map mappa = this.heapfile.getMap(midi);
        mappa.print();
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
        Set<String> distinctRow = new HashSet<>();
        mapVersion.keySet().forEach(key -> distinctRow.add(key.split("\\$")[0]));
        return distinctRow.size();
    }

    // Return number of distinct column labels in the bigtable.
    int getColumnCnt() {
        Set<String> distinctCol = new HashSet<>();
        mapVersion.keySet().forEach(key -> distinctCol.add(key.split("\\$")[1]));
        return distinctCol.size();
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

    // Prints the mapVersion HashMap
    private void printMapVersion(){
        mapVersion.entrySet().forEach(entry->{ System.out.println(entry.getKey() + ":" + entry.getValue().toString());});
    }


    // TODO: insert and return MID
    // This has to be modified to take care of storing 3 versions of a map at any point in time
    MID insertMap(byte[] mapPtr) throws Exception {
        MID mid = this.heapfile.insertMap(mapPtr);
        RID rid = MapUtils.ridFromMid(mid);
        Map map = new Map();
        map.setData(mapPtr);

        String key;

        String mapVersionKey = map.getRowLabel() + "$" + map.getColumnLabel();
        ArrayList<MID> list = mapVersion.get(mapVersionKey);
        if (list == null){
            list = new ArrayList<>();
        }
        else{
            // Add logic to keep only 3 records

        }
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