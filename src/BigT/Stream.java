package BigT;

import diskmgr.Page;
import global.*;

import java.io.IOException;
import heap.*;
import bufmgr.*;

public class Stream extends Scan  {

    Heapfile heapfile;
    /*
    Initialize a stream of maps on bigtable
    */


    /* Pending */
    public Stream(bigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws InvalidTupleSizeException, IOException {
        /* constructor */
        super(bigtable.heapfile);

        int mapCount = bigtable.getMapCnt();
        int rowCount = bigtable.getRowCnt();
        int columnCount = bigtable.getColumnCnt();
        this.heapfile = bigtable.heapfile;



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