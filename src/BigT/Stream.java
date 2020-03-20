package BigT;

import btree.*;
import diskmgr.bigDB;
import global.MID;
import heap.*;
import iterator.Sort;

import java.io.IOException;

/**
Initialize a stream of maps on bigtable.
 */
public class Stream {
    private final String rowFilter;
    private final String columnFilter;
    private final String valueFilter;
    private bigT bigtable;
    private Scan bigtScanner; // similar to heap scan @ashwin
    private boolean scanAll = false;
    String starFilter = new String("*");
    String rangeRegex = new String("\\[\\d+, \\d+\\]");
    private BTFileScan btreeScanner, dummyScanner;

    public Stream(bigT bigTable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {

        this.bigtable = bigTable;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        int type = 1;// this is the query type.
        queryConditions(type);
    }

    public void queryConditions(int type) throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException {

        StringKey start = null, end = null;

        switch(type) {
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

        btreeScanner = bigtable.indexFile.new_scan(start, end);
        dummyScanner = bigtable.indexFile.new_scan(null, null);
    }

    public void sortData() throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        Heapfile tempHeapFile = new Heapfile("query_temp_heap_file");

        if(scanAll) {
            //scanning whole bigt file.
            Map mapObj = bigtScanner.getNext(new MID()); // should be modified for mid.
        }
    }
}
