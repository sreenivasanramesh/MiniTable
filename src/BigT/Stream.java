package BigT;

import btree.*;
import diskmgr.OutOfSpaceException;
import global.AttrType;
import global.MID;
import global.RID;
import global.TupleOrder;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapSort;
import iterator.RelSpec;


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
    private int midCounter = 0;
    private MapSort sortObj;
    private boolean versionEnabled = true;
    private MapScan mapScan;
    private int type, orderType;


    public Stream(bigT bigTable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {

        this.bigtable = bigTable;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        this.type = bigTable.type;
        this.orderType = orderType;

        System.out.println("constructor");
        queryConditions();
        System.out.println("completed query conditions");
    }

    public void queryConditions() throws Exception {

        System.out.println("QC");

        StringKey start = null, end = null;

        /*
        type is an integer denoting the different clustering and indexing strategies you will use for the graph database.
         */
        switch(this.type) {
            case 1:
            default:
                // same as case 1
                this.scanAll = true;
                break;
            case 2:
                if (rowFilter.equals("*")) {
                    this.scanAll = true;
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
                if (columnFilter.equals("*")) {
                    System.out.println("column matches star");
                    this.scanAll = true;
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
                if ( (rowFilter.equals("*"))  && (columnFilter.equals("*"))) {
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
        System.out.println("outside QC break");

        if(!this.scanAll) {
            this.btreeScanner = bigtable.indexFile.new_scan(start, end);
        }

        System.out.println("qc before filter n sort");

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

        System.out.println("FS");
        tempHeapFile = new Heapfile(null);

        System.out.println("a");

        MID midObj = new MID();

        System.out.println(this.scanAll);


        if (this.scanAll) {
            System.out.println("if FS");


            //scanning whole bigt file.
            mapScan = bigtable.heapfile.openMapScan();
            System.out.println("openscan");

            //mapObj.setHeader();
            Map mapObj = null;

            if(rowFilter.equals(starFilter) && columnFilter.equals(columnFilter) && valueFilter.equals(starFilter)) {
                System.out.println("1 iffff");

                tempHeapFile = this.bigtable.heapfile;
            }
            else {
                System.out.println("2 elssss");

                mapObj = mapScan.getNext(midObj);
                while (mapObj != null) {
                    if (genericMatcher(mapObj, "row", rowFilter) && genericMatcher(mapObj, "column", columnFilter) && genericMatcher(mapObj, "value", valueFilter)) {
                        tempHeapFile.insertMap(mapObj.getMapByteArray());
                    }
                }
            }
        }
        else {
            System.out.println("else FS");

            KeyDataEntry entry = btreeScanner.get_next();
            while (entry != null) {
                RID rid = ((LeafData) entry.data).getData();
                if (rid != null) {
                    MID midFromRid = new MID(rid.pageNo, rid.slotNo);
                    Map map = bigtable.heapfile.getMap(midFromRid);
                    if(this.type == 5){
                        if((!rowFilter.matches(rangeRegex)) && !rowFilter.equals(map.getRowLabel())){
                            // is star
                            break;
                        } else if (rowFilter.matches(rangeRegex)) {
                            String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                            if((map.getRowLabel().compareTo(rowRange[0]) < 0)
                                    || (map.getRowLabel().compareTo(rowRange[1]) > 0)){
                                break;
                            }
                        }
                    }
                    tempHeapFile.insertMap(map.getMapByteArray());
                }
                entry = btreeScanner.get_next();
            }
        }

        System.out.println("FD proj");


            FldSpec[] projection = new FldSpec[4];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projection[0] = new FldSpec(rel, 1);
            projection[1] = new FldSpec(rel, 2);
            projection[2] = new FldSpec(rel, 3);
            projection[3] = new FldSpec(rel, 4);

            FileScan fscan = null;

            try {
                fscan = new FileScan("temp_heap_file", bigT.BIGT_ATTR_TYPES, bigT.BIGT_STR_SIZES, (short) 4, 4, projection, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        System.out.println("FSfilscan");
            int sortField, num_pages = 10;
            switch (orderType) {
                case 1:
                case 3:
                    sortField = 1;
                    break;
                case 2:
                case 4:
                    sortField = 2;
                    break;
                case 6:
                    sortField = 3;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + orderType);
            }
            try {
                System.out.println("reached till map sorter.");
                this.sortObj = new MapSort(bigT.BIGT_ATTR_TYPES, bigT.BIGT_STR_SIZES, fscan, sortField, new TupleOrder(TupleOrder.Ascending), num_pages);
                System.out.println("Came till filterAndSortData!!!");
            } catch (Exception e) {
                e.printStackTrace();
            }

    }


    public boolean genericMatcher(Map map, String field, String genericFilter) throws Exception {
        if (genericFilter.matches(rangeRegex)) {
            String[] range = genericFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getGenericValue(field).compareTo(range[0]) >= 0 && map.getGenericValue(field).compareTo(range[1]) <= 0) {
                // so now row is in range
                return true;
            }
            else {
                return false;
            }
        } else if(genericFilter.equals(map.getGenericValue(field))) {
            // matches specific value
            return true;
        } else if (genericFilter.equals(starFilter)){
            // matches star
            return false;
        }
        return false;
    }

    public boolean setFilter(Map map, String rowFilter, String columnFilter, String valueFilter) throws IOException {

        boolean ret_val = true;

        if (rowFilter.matches(rangeRegex)) {
            String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getRowLabel().compareTo(rowRange[0]) < 0 || map.getRowLabel().compareTo(rowRange[1]) > 0) ret_val = false;
        } else {
            //System.out.println("ganesh" + rowFilter + (rowFilter.matches(starFilter)));
            //System.out.println("suresh" + map.getRowLabel() + rowFilter);
            if (!(rowFilter.matches(starFilter))) {
                //ret_val = false;
                if(!map.getRowLabel().equals(rowFilter)) {
                    ret_val = false;
                }
            }
        }

        if (columnFilter.matches(rangeRegex)) {
            String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getRowLabel().compareTo(columnRange[0]) < 0 || map.getRowLabel().compareTo(columnRange[1]) > 0)
                ret_val = false;
        } else {
           // if ( (columnFilter.matches(starFilter))  && (map.getRowLabel().compareTo(columnFilter)!=0)) ret_val = false;
            if (!(columnFilter.matches(starFilter))) {
                //ret_val = false;
                if(!map.getColumnLabel().equals(columnFilter)) {
                    ret_val = false;
                }
            }
        }

        if (valueFilter.matches(rangeRegex)) {
            String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getRowLabel().compareTo(valueRange[0]) < 0 || map.getRowLabel().compareTo(valueRange[1]) > 0)
                ret_val = false;
        } else {
            //if ( (valueFilter.matches(starFilter))  && (map.getRowLabel().compareTo(valueFilter)!=0)) ret_val = false;
            if (!(valueFilter.matches(starFilter))) {
                //ret_val = false;
                if(!map.getValue().equals(valueFilter)) {
                    ret_val = false;
                }
            }
        }
        return ret_val;
    }


    public void closeStream() throws Exception {
        if (this.sortObj != null) {
            this.sortObj.close();
        }
        if (mapScan != null) {
            mapScan.closescan();
        }
        if (btreeScanner != null) {
            btreeScanner.DestroyBTreeFileScan();
        }
    }

    public Map getNext() throws Exception {
        if (this.sortObj == null) {
            System.out.println("sort object is not initialised");
            return null;
        }
        Map m = null;
        try {
            m = this.sortObj.get_next();
        } catch (OutOfSpaceException e) {
            closeStream();
        }
        if (m == null) {
            System.out.println("Map is null ");
            System.out.println("Deleting temp file used for sorting");
            tempHeapFile.deleteFile();
            closeStream();
            return null;
        }
        return m;
    }
}