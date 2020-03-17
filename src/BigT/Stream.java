package BigT;

import btree.IndexFileScan;
import global.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import heap.*;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;


public class Stream extends Scan {

    private static final boolean FAIL = false;
    /**
     * Initialize a stream of maps on big-table
     */
    Heapfile heapfile;
    String rowFilter;
    String columnFilter;
    String valueFilter;
    bigT bigTable;
    int orderType;
    int type;
    MID mid = null;
    String starFilter = new String("*");
    String rangeRegex = new String("\\[\\d+, \\d+\\]");
    private IndexFileScan btIndScan;


    /* Pending */
    public Stream(bigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter, int type) throws Exception {
        super(bigtable.heapfile);

        int mapCount = bigtable.getMapCnt();
        int rowCount = bigtable.getRowCnt();
        int columnCount = bigtable.getColumnCnt();
        this.bigTable = bigtable;
        this.orderType = orderType;
        this.heapfile = bigtable.heapfile;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        this.type = type;

    }


    /**
     * Closes open stream. Same as Heap.Scan.closeScan.
     */
    public void closeStream() {
        super.closescan();
    }


    /**
     * Retrieve the next map in the stream
     */
    public Map getNext(MID mid) throws Exception {
        return super.getNext(mid);
    }


    /*---------------------------------------------------------------------*/
    /*
    public int get_next_position() throws Exception {
        KeyDataEntry nextentry;
        try {
            nextentry = btIndScan.get_next();
        } catch (Exception e) {
            throw new Exception("IndexScan.java: BTree error");
        }

        if (nextentry == null)
            return -1;

        try {
            int position = ((LeafData) nextentry.data).getData();
            return position;
        } catch (Exception e) {
            throw new Exception("IndexScan.java: getRecord failed");
        }
    }
    */

    private List<Map> matchingElements(String field, String value, List<Map> mapsList) throws Exception {
        List<Map> matchingMaps = null;
        if (!field.matches("column|row|value")) {
            throw new Exception("Invalid field exception");
        }

        // check if index exists else the below code
        boolean indexExists = true;
        if (indexExists) {
            // create index scan object
            // get maps from index. and return list of maps
        } else {

            if (mapsList == null) {
                // use getnext for elements
                // foreach getnext do the matching
                System.out.println("just printing...");
            }

            for (int i = 0; i < mapsList.size(); i++) {
                if (mapsList.get(i).getGenericValue(field).matches(value)) {
                    matchingMaps.add(mapsList.get(i));
                }

            }
            return matchingMaps;
        }
    }

    private List<Map> matchingRange(String field, String rangeStr, List<Map> mapsList) throws Exception {
        List<Map> matchingMaps = null;
        List<String> rangeVal = Arrays.asList(rangeStr.replaceAll("[\\[ \\]]", "").split(","));
        if (!field.matches("column|row|value")) {
            throw new Exception("Invalid field exception");
        }

        if (mapsList != null) {
            for (int i = 0; i < mapsList.size(); i++) {
                String value = mapsList.get(i).getGenericValue(field);
                if (value.compareTo(rangeVal.get(0)) >= 0 && rangeVal.get(1).compareTo(value) <= 0) {
                    matchingMaps.add(mapsList.get(i));
                }
            }
        } else {
            Map mapObj = new Map();
            while (true) {
                mapObj = this.getNext(mid);
                if (mapObj == null) {
                    break;
                }
                String value = mapObj.getGenericValue(field);
                if (value.compareTo(rangeVal.get(0)) >= 0 && rangeVal.get(1).compareTo(value) <= 0) {
                    matchingMaps.add(mapObj);
                }
            }
        }
        return matchingMaps;
    }

    private List<Map> matchingAll(String field, List<Map> mapsList) throws Exception {
        List<Map> matchingMaps = null;
        if (!field.matches("column|row|value")) {
            throw new Exception("Invalid field exception");
        }
        if (mapsList == null) {
            Map mapObj = new Map();
            mapObj = this.getNext(mid);
            String value = mapObj.getGenericValue(field);
            matchingMaps.add(mapsList.get(i));
        }
        return mapsList;
    }

    /**
     * Based on type decide index
     * check if index exists for bigtable
     * if exists then query based on query conditions
     * else sequential scan.
     */
    public List<Map> StreamQuery() throws Exception {
        Map mapObj = new Map();
        List<Map> resList = new ArrayList<Map>();
        String[] indexNames;
        indexNames = this.bigTable.indexNames;
        switch (this.type) {
            case 1:
                // No index created. sequential scan.
                if (this.rowFilter.equals(starFilter)) {
                    // * row
                    if (this.colummatchingRangenFilter.matches(rangeRegex)) {
                        // * , range
                        // matches regex so split string and get first range
                        List<String> rangeValColumn = Arrays.asList(this.columnFilter.replaceAll("[\\[ \\]]", "").split(","));
                        do {
                            mapObj = this.getNext(mid);
                        } while (!mapObj.getColumnLabel().equals(rangeValColumn.get(0)));
                        do {
                            resList.add(mapObj);
                            mapObj = this.getNext(mid);
                        } while (!mapObj.getColumnLabel().equals(rangeValColumn.get(1)));
                        // now we have started from range and matched till range end and stored in mapList

                        // now check val is * or range or string
                        if (this.valueFilter.matches(rangeRegex)) {
                            List<String> rangeValValue = Arrays.asList(this.columnFilter.replaceAll("[\\[ \\]]", "").split(","));
                            List<Map> filteredMapList = new ArrayList<Map>();
                            int i;
                            for (i = 0; i < resList.size(); i++) {
                                Map tempMap = resList.get(i);
                                if (tempMap.getValue().matches(rangeValValue.get(0))) {
                                    filteredMapList.add(tempMap);
                                }
                            }
                            do {
                                i++;
                                Map tempMap = resList.get(i);
                                if (tempMap.getValue().matches(rangeValValue.get(1))) {
                                    return filteredMapList;
                                }
                                filteredMapList.add(tempMap);
                            } while (true);
                        } else if (this.valueFilter.matches(starFilter)) {
                            return resList;
                        } else {
                            // this is for *, range, value
                            List<Map> filteredMapList = new ArrayList<Map>();
                            for (Map temp : resList) {
                                if (this.valueFilter.matches(temp.getValue())) {
                                    filteredMapList.add(temp);
                                    return filteredMapList;
                                }
                            }
                        }
                    } else if (this.columnFilter.matches())
                        // specific element
                        while (true) {
                            mapObj = this.getNext(mid);
                            if (mapObj.getColumnLabel().equals(this.columnFilter)) {
                                break;
                            }
                        }
                }

        }
        break;
        case 2:
        break;
        case 3:
        break;
        case 4:
        break;
        case 5:
        break;
        default:
        throw new Exception("Invalid Type Passed");
    }



    protected void indexScanner(String rangeStart, String rangeEnd, String indexName) {
        System.out.println("------------------------ Index range scan --------------------------");

        // TODO: change temp values as per map
        short REC_LEN1 = 32;
        short REC_LEN2 = 160;
        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN2;
        attrSize[1] = REC_LEN1;

        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);


        // now try a range scan
        CondExpr[] expr = new CondExpr[3];
        expr[0] = new CondExpr();
        expr[0].op = new AttrOperator(AttrOperator.aopGE);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[0].operand2.string = rangeStart;
        expr[0].next = null;
        expr[1] = new CondExpr();
        expr[1].op = new AttrOperator(AttrOperator.aopLE);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[1].operand2.string = rangeEnd;
        expr[1].next = null;
        expr[2] = null;

        // start index scan
        IndexScan iScan = null;
        try {
            iScan = new IndexScan(new IndexType(IndexType.B_Index), indexName, "BTreeIndex", attrType, attrSize, 2, 2, projlist, expr, 2, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map tempObj = null;
        do {
            try {
                assert iScan != null;
                Tuple temp = iScan.get_next();
                tempObj = new Map();
                tempObj.setData(temp.getTupleByteArray());
                // should return map
                tempObj.print();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.err.println("------------------- index scan completed ---------------------\n");
        }while (tempObj != null);
    }
}