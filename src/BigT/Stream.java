package BigT;

import btree.*;
import diskmgr.OutOfSpaceException;
import global.AttrType;
import global.MID;
import global.RID;
import global.TupleOrder;
import heap.*;
import iterator.*;

import java.io.IOException;

/**
 Initialize a stream of maps on bigtable.
 */
public class Stream {
    private final String rowFilter;
    private final String columnFilter;
    private final String valueFilter;
    private bigT bigtable;
    private boolean scanAll = false;
    private String starFilter = new String("*");
    private String rangeRegex = new String("\\[\\d+, \\d+\\]");
    private BTFileScan btreeScanner, dummyScanner;
    public Heapfile tempHeapFile;
    private MID midList[];
    private int midCounter;
    private MapSort sortObj;
    private boolean versionEnabled = true;
    private MapScan mapScan;
    private int type, orderType;

    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private AttrType[] attrType = new AttrType[4];
    short[] attrSize = new short[4];


    public Stream(bigT bigTable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {

        this.bigtable = bigTable;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        this.type = bigTable.type;
        this.orderType = orderType;

        this.attrType[0] = new AttrType(AttrType.attrString);
        this.attrType[1] = new AttrType(AttrType.attrString);
        this.attrType[2] = new AttrType(AttrType.attrString);
        this.attrType[3] = new AttrType(AttrType.attrInteger); // TODO: string/int???

        this.attrSize[0] = REC_LEN1;
        this.attrSize[1] = REC_LEN1;
        this.attrSize[2] = REC_LEN1;
        this.attrSize[3] = REC_LEN1;

        queryConditions();
    }

    public void queryConditions() throws Exception {

        StringKey start = null, end = null;

        /*
        type is an integer denoting the different clustering and indexing strategies you will use for the graph database.
         */
        switch(this.type) {
            case 1:
            default:
                // same as case 1
                scanAll = true;
                break;
            case 2:
                if (rowFilter.matches(starFilter)) {
                    scanAll = true;
                } else {

                    // check if range
                    if (rowFilter.matches(rangeRegex)) {
                        String[] range = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(range[0]);
                        end = new StringKey(range[1]);
                    } else {
                        start = end = new StringKey(rowFilter);
                    }
                }
                break;
            case 3:
                if (columnFilter.matches(starFilter)) {
                    scanAll = true;
                } else {
                    // check if range
                    if (columnFilter.matches(rangeRegex)) {
                        String[] range = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(range[0]);
                        end = new StringKey(range[1]);
                    } else {
                        start = end = new StringKey(columnFilter);
                    }
                }
                break;
            case 4:
                if ( (rowFilter.matches(starFilter))  && (columnFilter.matches(starFilter))) {
                    scanAll = true;
                } else {

                    // check if both range
                    if ((rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {

                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        String [] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(rowRange[0] + columnRange[0]);
                        end = new StringKey(rowRange[1] + columnRange[1]);

                        //check row range and column fixed/*
                    } else if ((rowFilter.matches(rangeRegex)) && (!columnFilter.matches(rangeRegex))) {

                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey( rowRange[0] + columnFilter);
                        end =  new StringKey( rowRange[1] + columnFilter);

                        // check column range and row fixed/*
                    } else if ((!rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {

                        String [] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(rowFilter + columnRange[0]);
                        end = new StringKey(rowFilter + columnRange[1]);

                        //row and col are fixed val or *,fixed fixed,*
                    } else {

                        start = new StringKey(rowFilter + columnFilter);
                        end = start;
                    }
                }
                break;
            case 5:
                if (rowFilter.matches(starFilter)) {
                    scanAll = true;
                } else {
                    if (valueFilter.matches(starFilter)) {
                        start = new StringKey(rowFilter);
                    } else {
                        if ((rowFilter.matches(rangeRegex)) && (valueFilter.matches(rangeRegex))) {

                            String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                            String [] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
                            start = new StringKey(rowRange[0] + valueRange[0]);
                            end = new StringKey(rowRange[1] + valueRange[1]);

                        } else {

                            start = new StringKey(rowFilter + valueFilter);
                            end = new StringKey(rowFilter + valueFilter);

                        }// add other conditions
                    }
                }
                break;
        }

        this.btreeScanner = bigtable.indexFile.new_scan(start, end);
        dummyScanner = bigtable.indexFile.new_scan(null, null);
        filterAndSortData(this.orderType);
    }

    public void filterAndSortData(int orderType) throws Exception {
        /* orderType is for ordering by
        · 1, then results are first ordered in row label, then column label, then time stamp
        · 2, then results are first ordered in column label, then row label, then time stamp
        · 3, then results are first ordered in row label, then time stamp
        · 4, then results are first ordered in column label, then time stamp
        · 6, then results are ordered in time stamp
        * */
        tempHeapFile = new Heapfile("query_temp_heap_file");
        MID midObj = new MID();
        if (scanAll) {
            //scanning whole bigt file.
            mapScan = bigtable.heapfile.openMapScan();
            //bigtable.heapfile.openMapScan();
            //mapObj.setHeader();
            Map mapObj = null;

            do {
                mapObj = mapScan.getNext(midObj);
                // TODO: not sure if need to set header
                boolean set_filter = setFilter(mapObj, rowFilter, columnFilter, valueFilter);
                if (set_filter) {
                    if (orderType == 6 && midCounter < 3) {
                        MID tempMid = new MID(midObj.getPageNo(), midObj.getSlotNo());
                        midList[midCounter++] = tempMid;
                    }
                }
            }while (mapObj != null) ;
        } else {
            KeyDataEntry entry = btreeScanner.get_next();
            while (entry != null) {
                RID rid = ((LeafData) entry.data).getData();
                if (rid != null) {
                    MID midFromRid = new MID(rid.pageNo, rid.slotNo);
                    Map map = bigtable.heapfile.getMap(midFromRid);
                    if(this.type == 5){
                        if((!rowFilter.matches(rangeRegex)) && rowFilter.compareTo(map.getRowLabel()) != 0){
                            break;
                        } else if (rowFilter.matches(rangeRegex)) {
                            String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                            if((map.getRowLabel().compareTo(rowRange[0]) < 0)
                                    || (map.getRowLabel().compareTo(rowRange[1]) > 0)){
                                break;
                            }
                        }
                    }
                    boolean set_filter = setFilter(map, rowFilter, columnFilter, valueFilter);
                    if (set_filter) {
                        if (orderType == 6 && midCounter < 3) {
                            MID tempMid = new MID(midObj.getPageNo(), midObj.getSlotNo());
                            midList[midCounter++] = tempMid;
                        }
                    }
                }
                entry = btreeScanner.get_next();
            }
        }

        if (versionEnabled) {
            FldSpec[] projection = new FldSpec[4];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projection[0] = new FldSpec(rel, 1);
            projection[1] = new FldSpec(rel, 2);
            projection[2] = new FldSpec(rel, 3);
            projection[3] = new FldSpec(rel, 4);

            FileScan fscan = null;

            try {
                // TODO : set attribute types and attribute sizes - done
                fscan = new FileScan("query_temp_heap_file", attrType, attrSize, (short) 4, 4, projection, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
            int sortField, maxLength = -1;
            // TODO: set max length.
            switch (orderType) {
                case 1:
                case 3:
                    sortField = 1;
                    maxLength = this.bigtable.getRowCnt();
                    break;
                case 2:
                case 4:
                    sortField = 2;
                    maxLength = this.bigtable.getColumnCnt();
                    break;
                case 6:
                    sortField = 3;
                    maxLength = this.bigtable.getTimeStampCnt();
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + orderType);
            }
            try {
                sortObj = new MapSort(attrType,attrSize, fscan, sortField, new TupleOrder(TupleOrder.Ascending), maxLength);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public boolean setFilter(Map map, String rowFilter, String columnFilter, String valueFilter) throws IOException {

        boolean ret_val = true;

        if (rowFilter.matches(rangeRegex)) {
            String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getRowLabel().compareTo(rowRange[0]) < 0 || map.getRowLabel().compareTo(rowRange[1]) > 0) ret_val = false;
        } else {
            if ( (rowFilter.matches(starFilter))  && (map.getRowLabel().compareTo(rowFilter)!=0)) ret_val = false;
        }

        if (columnFilter.matches(rangeRegex)) {
            String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getRowLabel().compareTo(columnRange[0]) < 0 || map.getRowLabel().compareTo(columnRange[1]) > 0)
                ret_val = false;
        } else {
            if ( (columnFilter.matches(starFilter))  && (map.getRowLabel().compareTo(columnFilter)!=0)) ret_val = false;
        }

        if (valueFilter.matches(rangeRegex)) {
            String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getRowLabel().compareTo(valueRange[0]) < 0 || map.getRowLabel().compareTo(valueRange[1]) > 0)
                ret_val = false;
        } else {
            if ( (valueFilter.matches(starFilter))  && (map.getRowLabel().compareTo(valueFilter)!=0)) ret_val = false;
        }
        return ret_val;
    }


    public void closeStream() throws Exception {
        if (sortObj != null) {
            sortObj.close();
        }
        if (mapScan != null) {
            mapScan.closescan();
        }
        if (btreeScanner != null) {
            btreeScanner.DestroyBTreeFileScan();
        }
    }

    public Map getNext() throws Exception {
        if (sortObj == null) {
            System.out.println("something is wrong, might be check versions flag is enabled");
            return null;
        }
        Map m = null;
        try {
            m = sortObj.get_next();
        } catch (OutOfSpaceException e) {
            closeStream();
        }
        if (m == null) {
            System.out.println("Deleting temp file used for sorting");
            tempHeapFile.deleteFile();
            closeStream();
            return null;
        }
        return m;
    }
}