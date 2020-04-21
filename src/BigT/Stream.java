package BigT;

import btree.BTFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import cmdline.MiniTable;
import diskmgr.OutOfSpaceException;
import global.MID;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.MapScan;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapSort;
import iterator.RelSpec;

import java.io.IOException;

/**
 * Stream Class to initialize a stream of maps on bigTable.
 */
public class Stream {
    private final String rowFilter;
    private final String columnFilter;
    private final String valueFilter;
    private bigT bigtable;
    private boolean scanAll = false;
    private String starFilter;
    private String rangeRegex = "\\[\\S+,\\S+\\]";
    // Used to include the high filter also while fetching from BTree Index
    private String lastChar;
    private BTFileScan btreeScanner, dummyScanner;
    public Heapfile tempHeapFile;
    private MID[] midList;
    private int midCounter = 0;
    private MapSort sortObj;
    private boolean versionEnabled = true;
    private MapScan mapScan;
    private int type, orderType;


    /**
     * @param bigTable Object containing all the maps in bigt table.
     * @param orderType Ordertype for sorting/ ordering of output.
     * @param rowFilter Filtering based on row values.
     * @param columnFilter Filtering based on column values.
     * @param valueFilter Filtering based on value field.
     * @throws Exception Throws generic Exception.
     */
    public Stream(bigT bigTable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {

        this.bigtable = bigTable;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        this.type = bigTable.type;
        this.orderType = orderType;
        this.starFilter = "*";
        this.lastChar = "Z";


        queryConditions();
        filterAndSortData(this.orderType);



    }

    /**
     * This type value for each type to use index or file.
     * @throws Exception Throws generic exception.
     */
    public void queryConditions() throws Exception {


        StringKey start = null, end = null;

        /*
        type is an integer denoting the different clustering and indexing strategies you will use for the graph database.
         */
        switch (this.type) {
            case 1:
            default:
                // same as case 1
                this.scanAll = true;
                break;
            case 2:
                if (rowFilter.equals(starFilter)) {
                    this.scanAll = true;
                } else {
                    // check if range
                    if (rowFilter.matches(rangeRegex)) {
                        String[] range = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(range[0]);
                        end = new StringKey(range[1] + this.lastChar);
                    } else {
                        start = new StringKey(rowFilter);
                        end = new StringKey(rowFilter + this.lastChar);
                    }
                }
                break;
            case 3:
                if (columnFilter.equals("*")) {
                    this.scanAll = true;
                } else {
                    // check if range
                    if (columnFilter.matches(rangeRegex)) {
                        String[] range = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(range[0]);
                        end = new StringKey(range[1] + this.lastChar);
                    } else {
                        start = new StringKey(columnFilter);
                        end = new StringKey(columnFilter + this.lastChar);
                    }
                }
                break;
            case 4:
                if ((rowFilter.equals("*")) && (columnFilter.equals("*"))) {
                    scanAll = true;
                } else {

                    // check if both range
                    if ((rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {

                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(columnRange[0] + "$" + rowRange[0]);
                        end = new StringKey(columnRange[1] + "$" + rowRange[1] + this.lastChar);

                        //check row range and column fixed/*
                    } else if ((rowFilter.matches(rangeRegex)) && (!columnFilter.matches(rangeRegex))) {
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (columnFilter.equals(starFilter)) {
                            scanAll = true;
                        } else {
                            start = new StringKey(columnFilter + "$" + rowRange[0]);
                            end = new StringKey(columnFilter + "$" + rowRange[1] + this.lastChar);
                        }
                        // check column range and row fixed/*
                    } else if ((!rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {
                        String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (rowFilter.equals(starFilter)) {
                            start = new StringKey(columnRange[0]);
                            end = new StringKey(columnRange[1] + this.lastChar);
                        } else {

                            start = new StringKey(columnRange[0] + "$" + rowFilter);
                            end = new StringKey(columnRange[1] + "$" + rowFilter + this.lastChar);
                        }

                        //row and col are fixed val or *,fixed fixed,*
                    } else {
                        if (columnFilter.equals(starFilter)) {
                            scanAll = true;
                        } else if (rowFilter.equals(starFilter)) {
                            start = end = new StringKey(columnFilter);
                        } else {
                            start = new StringKey(columnFilter + "$" + rowFilter);
                            end = new StringKey(columnFilter + "$" + rowFilter + this.lastChar);
                        }
                    }
                }
                break;
            case 5:
                
                if ((valueFilter.equals(starFilter)) && (rowFilter.equals(starFilter))) {
                    scanAll = true;
                } else {

                    // check if both range
                    if ((valueFilter.matches(rangeRegex)) && (rowFilter.matches(rangeRegex))) {

                        String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(rowRange[0] + "$" + valueRange[0]);
                        end = new StringKey(rowRange[1] + "$" + valueRange[1] + this.lastChar);

                        //check row range and column fixed/*
                    } else if ((valueFilter.matches(rangeRegex)) && (!rowFilter.matches(rangeRegex))) {
                        String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (rowFilter.equals(starFilter)) {
                            scanAll = true;
                        } else {
                            start = new StringKey(rowFilter + "$" + valueRange[0]);
                            end = new StringKey(rowFilter + "$" + valueRange[1] + this.lastChar);
                        }
                        // check column range and row fixed/*
                    } else if ((!valueFilter.matches(rangeRegex)) && (rowFilter.matches(rangeRegex))) {
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (valueFilter.equals("*")) {
                            start = new StringKey(rowRange[0]);
                            end = new StringKey(rowRange[1] + this.lastChar);
                        } else {

                            start = new StringKey(rowRange[0] + "$" + valueFilter);
                            end = new StringKey(rowRange[1] + "$" + valueFilter + this.lastChar);
                        }

                        //row and col are fixed val or *,fixed fixed,*
                    } else {
                        if (rowFilter.equals("*")) {
                            // *, fixed
                            scanAll = true;
                        } else if (valueFilter.equals("*")) {
                            // fixed, *
                            start = new StringKey(rowFilter);
                            end = new StringKey(rowFilter + lastChar);
                        } else {
                            // both fixed
                            start = new StringKey(rowFilter + "$" + valueFilter);
                            end = new StringKey(rowFilter + "$" + valueFilter + this.lastChar);
                        }
                    }
                }
                break;
        }

        if (!this.scanAll) {
            this.btreeScanner = bigtable.indexFile.new_scan(start, end);
        }


    }

    /**
     * @param orderType Ordertype for sorting data.
     * @throws Exception
     */
    public void filterAndSortData(int orderType) throws Exception {
        /* orderType is for ordering by
        · 1, then results are first ordered in row label, then column label, then time stamp
        · 2, then results are first ordered in column label, then row label, then time stamp
        · 3, then results are first ordered in row label, then time stamp
        · 4, then results are first ordered in column label, then time stamp
        · 6, then results are ordered in time stamp
        * */

        tempHeapFile = new Heapfile("tempSort4");

        MID midObj = new MID();
        if (this.scanAll) {
            //scanning whole bigt file.
            mapScan = bigtable.heapfile.openMapScan();

            //mapObj.setHeader();
            Map mapObj = null;

//            if (rowFilter.equals(starFilter) && columnFilter.equals(starFilter) && valueFilter.equals(starFilter)) {
//                System.out.println("rowFilter = " + rowFilter);
//                tempHeapFile = this.bigtable.heapfile;
//            } else {

            int count = 0;
            mapObj = this.mapScan.getNext(midObj);
            while (mapObj != null) {
                count++;
                short kaka = 0;
                if (genericMatcher(mapObj, "row", rowFilter) && genericMatcher(mapObj, "column", columnFilter) && genericMatcher(mapObj, "value", valueFilter)) {
                    tempHeapFile.insertMap(mapObj.getMapByteArray());
                }
                mapObj = mapScan.getNext(midObj);
            }

        } else {

            KeyDataEntry entry = btreeScanner.get_next();
            while (entry != null) {
                RID rid = ((LeafData) entry.data).getData();
                if (rid != null) {
                    MID midFromRid = new MID(rid.pageNo, rid.slotNo);
                    Map mapObj = bigtable.heapfile.getMap(midFromRid);
                    if (genericMatcher(mapObj, "row", rowFilter) && genericMatcher(mapObj, "column", columnFilter) && genericMatcher(mapObj, "value", valueFilter)) {
                        tempHeapFile.insertMap(mapObj.getMapByteArray());
                    }

                }
                entry = btreeScanner.get_next();
            }
        }


        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        FileScan fscan = null;

        try {
            fscan = new FileScan("tempSort4", MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int sortField, num_pages = 10, sortFieldLength;
        switch (orderType) {
            case 1:
            case 3:
                sortField = 1;
                sortFieldLength = MiniTable.BIGT_STR_SIZES[0];
                break;
            case 2:
            case 4:
                sortField = 2;
                sortFieldLength = MiniTable.BIGT_STR_SIZES[1];
                break;
            case 5:
                sortField = 3;
                sortFieldLength = MiniTable.BIGT_STR_SIZES[2];
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + orderType);
        }
        try {
            this.sortObj = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, fscan, sortField, new TupleOrder(TupleOrder.Ascending), num_pages, sortFieldLength);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * @param map Map object to compare field with filter.
     * @param field field type to be compared - row, column or value.
     * @param genericFilter Filter on fields - row, column or value.
     * @return
     * @throws Exception
     */
    private boolean genericMatcher(Map map, String field, String genericFilter) throws Exception {
        if (genericFilter.matches(rangeRegex)) {
            String[] range = genericFilter.replaceAll("[\\[ \\]]", "").split(",");
//            String[] range = ",".split(genericFilter.replaceAll("[\\[ \\]]", ""));
            // so now row is in range
//            System.out.println("range = " + Arrays.toString(range));
            return map.getGenericValue(field).compareTo(range[0]) >= 0 && map.getGenericValue(field).compareTo(range[1]) <= 0;
        } else if (genericFilter.equals(map.getGenericValue(field))) {

//                System.out.println("genericFilter = " + genericFilter);
//                System.out.println("map.getGenericValue() = " + map.getGenericValue(field));
            // matches specific value
            return true;
        } else {
            return genericFilter.equals(starFilter);
        }
    }

    public boolean setFilter(Map map, String rowFilter, String columnFilter, String valueFilter) throws IOException {

        boolean ret_val = true;

        if (rowFilter.matches(rangeRegex)) {
            String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
            if (map.getRowLabel().compareTo(rowRange[0]) < 0 || map.getRowLabel().compareTo(rowRange[1]) > 0)
                ret_val = false;
        } else {
            //System.out.println("ganesh" + rowFilter + (rowFilter.matches(starFilter)));
            //System.out.println("suresh" + map.getRowLabel() + rowFilter);
            if (!(rowFilter.matches(starFilter))) {
                //ret_val = false;
                if (!map.getRowLabel().equals(rowFilter)) {
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
                if (!map.getColumnLabel().equals(columnFilter)) {
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
                if (!map.getValue().equals(valueFilter)) {
                    ret_val = false;
                }
            }
        }
        return ret_val;
    }


    /**
     * Closes the stream object.
     * @throws Exception Throws generic exception.
     */
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
        tempHeapFile.deleteFile();
    }

    /**
     * @return returns the Map in the Stream based on query conditions.
     * @throws Exception throws generic exception.
     */
    public Map getNext() throws Exception {
        if (this.sortObj == null) {
            System.out.println("sort object is not initialised");
            return null;
        }
        Map m = null;
        try {
            m = this.sortObj.get_next();

        } catch (OutOfSpaceException e) {
            System.out.println("outofspace");
            e.printStackTrace();
            closeStream();
        }
        if (m == null) {
            closeStream();
            return null;
        }
        return m;
    }
}