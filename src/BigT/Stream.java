package BigT;

import global.MID;
import java.io.IOException;
import heap.Scan;
import heap.Heapfile;
import heap.InvalidTupleSizeException;


public class Stream extends Scan  {

    /** Initialize a stream of maps on big-table */
    Heapfile heapfile;
    String rowFilter;
    String columnFilter;
    String valueFilter;
    bigT bigTable;
    int orderType;
    int type;
    String starFilter = new String("*");
    String rangeRegex = new String("\\[\\d+, \\d+\\]");


    /* Pending */
    public Stream(bigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter, int type) throws InvalidTupleSizeException, IOException {
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

        /** Based on type decide index
         * check if index exists for bigtable
         * if exists then query based on query conditions
         * else sequential scan.
         */
        
    }


    /**
     *  Closes open stream. Same as Heap.Scan.closeScan.
     */
    public void closeStream() {
        super.closescan();
    }


    /**
    Retrieve the next map in the stream
    */
    public Map getNext(MID mid) throws Exception {
        return super.getNext(mid);
    }

}