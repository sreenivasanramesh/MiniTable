package BigT;

import global.GlobalConst;
import global.MID;
import global.PageId;
import heap.*;

import java.io.IOException;

public class Stream implements GlobalConst {

	/*
	Initialize a stream of maps on bigtable
	*/

    /**
     * The heapfile we are using.
     */
    private Heapfile _hf;

    /**
     * PageId of current directory page (which is itself an HFPage)
     */
    private PageId dirpageId = new PageId();

    /**
     * pointer to in-core data of dirpageId (page is pinned)
     */
    private HFPage dirpage = new HFPage();

    /**
     * Map ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current map is present.
     */
    private MID datapageMid = new MID();

    /**
     * the actual PageId of the data page with the current record
     */
    private PageId datapageId = new PageId();

    /**
     * in-core copy (pinned) of the same
     */
    private HFPage datapage = new HFPage();

    /**
     * map ID of the current map (from the current data page)
     */
    private MID currentMid = new MID();

    /**
     * Status of next user status
     */
    private boolean nextUserStatus;


    /* Pending */
    public Stream(bigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, InvalidSlotNumberException, IOException {
        /* constructor */
        int mapCount = bigtable.getMapCnt();
        int rowCOunt = bigtable.getRowCnt();
        int columnCount = bigtable.getColumnCnt();
    }

    /*
    Closes the stream object.
    */
    public void closeStream() {
        reset();
    }

    /*
    Retrieve the next map in the stream
    */
    public Map getNext(MID MID) throws Exception {

        Map nextMap = null;

        /* nextUserStatus defined in class */

        if (!nextUserStatus) {
            /* TODO: modify next data page according to map instead of tuple */
//            nextDataPage();
        }

        /* heap file page - defined in class */
        if (datapage == null) {
            return null;
        }


        /* CurrentMid is declared in class
         not sure why copied, need tobe verified
         */
        MID.copyMid(currentMid);

        try {
            // get next map similar to recptrtuple = datapage.getRecord(MID);
            nextMap = datapage.getMap(MID);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // check next map entry exists
//        currentMid = datapage.nextMap(MID);

        nextUserStatus = currentMid != null;

        return nextMap;
    }

    /**
     * to be implemented
     * - datapage.nextMap(mid); - added
     * - datapage.getMap(mid); - added but need to verify functionality
     * - nextDataPage(); to be modified to work for map instead of tuple
     */








    /*
    Reset is fine - need to add scan methods import
    */
    private void reset() {

        if (datapage != null) {

            try {
//                unpinPage(datapageId, false);
            } catch (Exception e) {
                // 	System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null) {

            try {
//                unpinPage(dirpageId, false);
            } catch (Exception e) {
                //     System.err.println("SCAN: Error in Scan: " + e);
                e.printStackTrace();
            }
        }
        dirpage = null;

        nextUserStatus = true;

    }
}