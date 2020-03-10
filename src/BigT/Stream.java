package BigT;

import diskmgr.Page;
import global.GlobalConst;
import global.MID;
import global.PageId;
import global.SystemDefs;
import java.io.IOException;
import heap.*;
import bufmgr.*;

public class Stream implements GlobalConst  {

	/*
	Initialize a stream of maps on bigtable
	*/


    private Heapfile heapfile;

    private PageId dirpageId = new PageId();
    private HFPage dirpage = new HFPage(); //pointer to pinned dir page
    private MID mid = new MID();
    private PageId datapageId = new PageId();
    private HFPage datapage = new HFPage(); //pointer to pinned data page
    private MID currentMid = new MID();
    private boolean nextUserStatus;


    /* Pending */
    public Stream(bigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) {
        /* constructor */
        int mapCount = bigtable.getMapCnt();
        int rowCOunt = bigtable.getRowCnt();
        int columnCount = bigtable.getColumnCnt();

        this.heapfile = bigtable.heapfile;



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
    public Map getNext(MID mid) throws Exception {

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
        currentMid = datapage.nextMap(MID);

        nextUserStatus = currentMid != null;

        return nextMap;
    }

    /**
     * to be implemented
     * - datapage.nextMap(mid); - added
     * - datapage.getMap(mid); - added but need to verify functionality
     * - nextDataPage(); to be modified to work for map instead of tuple
     */



    private boolean firstDataPage()
            throws InvalidTupleSizeException,
            IOException {


        DataPageInfo dpinfo;
        Map map = null;
        Boolean bst;

        /** copy data about first directory page */
        dirpageId.pid = this.headerfile._firstDirPageId.pid;
        nextUserStatus = true;

        try{
            dirpage = new HFPage();
            pinPage(dirpageId, (Page) dirpage, false);
            datapageRid = dirpage.firstRecord();

            /** there is a datapage record on the first directory page: */
            if (datapageRid != null) {
                map = dirpage.getRecord(datapageRid);
                //to set datapageId
                dpinfo = new DataPageInfo(map);
                datapageId.pid = dpinfo.pageId.pid;

            }
            else {
                // the first directory page is the only one which can possibly remain empty
                // we get the next directory page and check it, unless heapfile is empty
                PageId nextDirPageId = new PageId();
                nextDirPageId = dirpage.getNextPage();
                if (nextDirPageId.pid != INVALID_PAGE) {

                    unpinPage(dirpageId, false);
                    dirpage = null;
                    dirpage = new HFPage();
                    pinPage(nextDirPageId, (Page) dirpage, false);


                    /** now try again to read a data record: */
                    try {
                        datapageRid = dirpage.firstRecord();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        datapageId.pid = INVALID_PAGE;
                    }

                    if (datapageRid != null) {
                        map = dirpage.getRecord(datapageRid);
                        if (map.getLength() != DataPageInfo.size)
                            return false;

                        dpinfo = new DataPageInfo(map);
                        datapageId.pid = dpinfo.pageId.pid;
                    }
                    else {
                        // heapfile empty
                        datapageId.pid = INVALID_PAGE;
                    }
                }
                else {
                    // heapfile empty
                    datapageId.pid = INVALID_PAGE;
                }
            }

            datapage = null;

            nextDataPage();

        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return true;

        /** ASSERTIONS:
         * - first directory page pinned
         * - this->dirpageId has Id of first directory page
         * - this->dirpage valid
         * - if heapfile empty:
         *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
         * - if heapfile nonempty:
         *    - this->datapage == NULL, this->datapageId, this->datapageRid valid
         *    - first datapage is not yet pinned
         */

    }


    /*
    Reset is fine - need to add scan methods import
    */
    private void reset() {

        if (datapage != null) {

            try {
                unpinPage(datapageId, false);
            } catch (Exception e) {
                // 	System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null) {

            try {
                unpinPage(dirpageId, false);
            } catch (Exception e) {
                //     System.err.println("SCAN: Error in Scan: " + e);
                e.printStackTrace();
            }
        }
        dirpage = null;

        nextUserStatus = true;

    }


    /**
     * short cut to access the pinPage function in bufmgr package.
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Scan.java: pinPage() failed");
        }
    }

    /**
     * short cut to access the unpinPage function in bufmgr package.
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Scan.java: unpinPage() failed");
        }
    }


}