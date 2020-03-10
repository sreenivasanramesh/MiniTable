package BigT;

import diskmgr.Page;
import global.*;

import java.io.IOException;
import heap.*;
import bufmgr.*;

public class Stream extends Scan  {

    /*
    Initialize a stream of maps on bigtable
    */
    Heapfile heapfile;
    String rowFilter;
    String columnFilter;
    String valueFilter;


    /* Pending */
    public Stream(bigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws InvalidTupleSizeException, IOException {
        /* constructor */
        super(bigtable.heapfile);

        int mapCount = bigtable.getMapCnt();
        int rowCount = bigtable.getRowCnt();
        int columnCount = bigtable.getColumnCnt();
        this.heapfile = bigtable.heapfile;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        String star_filter = new String("*");
        Heapfile heapfile = bigtable.heapfile;
        String rangeRegex = new String("\\[\\d+, \\d+\\]");
        if (rowFilter.matches(rangeRegex)) {

        }
        else {
            // should match single element.
        }

        }

        if (star_filter.equals(rowFilter)) {
            if (star_filter.equals(columnFilter)) {
                if(star_filter.equals(valueFilter)) {
                    // row, col, val = *, * , * - so sequential scan on all elements
                    //no changes required, scan already pointing to first mid.
                }

            }
    }


    public void closeStream() {
        super.closescan();
    }

    /*
    Retrieve the next map in the stream
    */
    public Map getNext(MID mid) throws Exception {
        return super.getNext(mid);
    }

    public Tuple getNext(RID rid)
            throws InvalidTupleSizeException,
            IOException {
        Tuple recptrtuple = null;

        boolean nextUserStatus = super.getNextUserStatus();
        HFPage datapage = super.getDataPage();
        RID userrid = super.getUserId();

        if (nextUserStatus != true) {
            super.nextDataPage();
        }

        if (datapage == null)
            return null;

        rid.pageNo.pid = userrid.pageNo.pid;
        rid.slotNo = userrid.slotNo;

        try {
            recptrtuple = datapage.getRecord(rid);
        } catch (Exception e) {
            //System.err.println("SCAN: Error in Scan" + e);
            e.printStackTrace();
        }

        super.setUserId(datapage.nextRecord(rid));
        if (userrid == null) super.setNextUserStatus(false);
        else super.setNextUserStatus(true);

        return recptrtuple;
    }





}