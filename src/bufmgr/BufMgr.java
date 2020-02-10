/*  File BufMgr,java */

package bufmgr;

import java.io.*;
import java.util.*;

import diskmgr.*;
import global.*;


/**
 * A frame description class. It describes each page in the buffer
 * pool, the page number in the file, whether it is dirty or not,
 * its pin count, and the pin count change when pinning or unpinning
 * a page.
 */
class FrameDesc implements GlobalConst {

    /**
     * The page within file, or INVALID_PAGE if the frame is empty.
     */
    public PageId pageNo;

    /**
     * the dirty bit, 1 (TRUE) stands for this frame is altered,
     * 0 (FALSE) for clean frames.
     */
    public boolean dirty;

    /**
     * The pin count for the page in this frame
     */
    public int pin_cnt;

    /**
     * Creates a FrameDesc object, initialize pageNo, dirty and
     * pin_count.
     */
    public FrameDesc() {

        pageNo = new PageId();
        pageNo.pid = INVALID_PAGE;
        dirty = false;
        pin_cnt = 0;

    }


    /**
     * Returns the pin count of a certain frame page.
     *
     * @return the pin count number.
     */
    public int pin_count() {
        return (pin_cnt);
    }

    /**
     * Increments the pin count of a certain frame page when the
     * page is pinned.
     *
     * @return the incremented pin count.
     */
    public int pin() {
        return (++pin_cnt);
    }

    /**
     * Decrements the pin count of a frame when the page is
     * unpinned.  If the pin count is equal to or less than
     * zero, the pin count will be zero.
     *
     * @return the decremented pin count.
     */
    public int unpin() {

        pin_cnt = (pin_cnt <= 0) ? 0 : pin_cnt - 1;

        return (pin_cnt);
    }
}


// *****************************************************

/**
 * A buffer hashtable entry description class. It describes
 * each entry for the buffer hash table, the page number and
 * frame number for that page, the pointer points to the next
 * hash table entry.
 */
class BufHTEntry {
    /**
     * The next entry in this hashtable bucket.
     */
    public BufHTEntry next;

    /**
     * This page number.
     */
    public PageId pageNo = new PageId();

    /**
     * The frame we are stored in.
     */
    public int frameNo;
}


// *****************************************************

/**
 * A buffer hashtable to keep track of pages in the buffer pool.
 * It inserts, retrieves and removes pages from the h ash table.
 */
class BufHashTbl implements GlobalConst {


    /**
     * Hash Table size, small number for debugging.
     */
    private static final int HTSIZE = 20;


    /**
     * Each slot holds a linked list of BufHTEntrys, NULL means
     * none.
     */
    private BufHTEntry ht[] = new BufHTEntry[HTSIZE];


    /**
     * Returns the number of hash bucket used, value between 0 and HTSIZE-1
     *
     * @param pageNo the page number for the page in file.
     * @return the bucket number in the hash table.
     */
    private int hash(PageId pageNo) {
        return (pageNo.pid % HTSIZE);
    }


    /**
     * Creates a buffer hash table object.
     */
    public BufHashTbl() {
        for (int i = 0; i < HTSIZE; i++)
            ht[i] = null;
    }


    /**
     * Insert association between page pageNo and frame frameNo
     * into the hash table.
     *
     * @param pageNo  page number in the bucket.
     * @param frameNo frame number in the bucket.
     * @return true if successful.
     */
    public boolean insert(PageId pageNo, int frameNo) {

        BufHTEntry ent = new BufHTEntry();
        int index = hash(pageNo);

        ent.pageNo.pid = pageNo.pid;
        ent.frameNo = frameNo;

        ent.next = ht[index];    // insert this page at the top
        ht[index] = ent;

        return true;
    }


    /**
     * Find a page in the hashtable, return INVALID_PAGE
     * on failure, otherwise the frame number.
     *
     * @param pageNo page number in the bucket.
     */
    public int lookup(PageId pageNo) {

        BufHTEntry ent;
        if (pageNo.pid == INVALID_PAGE)
            return INVALID_PAGE;

        for (ent = ht[hash(pageNo)]; ent != null; ent = ent.next) {
            if (ent.pageNo.pid == pageNo.pid) {
                return (ent.frameNo);
            }
        }

        return (INVALID_PAGE);

    }

    /**
     * Remove the page from the hashtable.
     *
     * @param pageNo page number of the bucket.
     */
    public boolean remove(PageId pageNo) {

        BufHTEntry cur, prev = null;

        // Allow INVALID_PAGE to be removed all they want.
        if (pageNo.pid == INVALID_PAGE)
            return true;

        int indx = hash(pageNo);
        for (cur = ht[indx]; cur != null; cur = cur.next) {

            if (cur.pageNo.pid == pageNo.pid)
                break;
            prev = cur;
        }

        if (cur != null) {
            if (prev != null)
                prev.next = cur.next;
            else
                ht[indx] = cur.next;

        } else {
            System.err.println("ERROR: Page " + pageNo.pid
                    + " was not found in hashtable.\n");

            return false;
        }

        return true;

    }

    /**
     * Show hashtable contents.
     */
    public void display() {
        BufHTEntry cur;

        System.out.println("HASH Table contents :FrameNo[PageNo]");

        for (int i = 0; i < HTSIZE; i++) {
            //   System.out.println ( "\nindex: " + i + "-" );
            if (ht[i] != null) {

                for (cur = ht[i]; cur != null; cur = cur.next) {
                    System.out.println(cur.frameNo + "[" + cur.pageNo.pid + "]-");
                }
                System.out.println("\t\t");

            } else {
                System.out.println("NONE\t");
            }
        }
        System.out.println("");

    }

}

// *****************************************************

/**
 * A clock algorithm for buffer pool replacement policy.
 * It picks up the frame in the buffer pool to be replaced.
 * This is the default replacement policy.
 */
class Clock extends Replacer {

    /**
     * Creates a clock object.
     */
    public Clock(BufMgr javamgr) {
        super(javamgr);

    }

    /**
     * Picks up the victim frame to be replaced according to
     * the clock algorithm.  Pin the victim so that other
     * process can not pick it as a victim.
     *
     * @return -1 if no frame is available.
     * head of the list otherwise.
     * @throws BufferPoolExceededException.
     */
    public int pick_victim()
            throws BufferPoolExceededException,
            PagePinnedException {
        int num = 0;
        int numBuffers = mgr.getNumBuffers();

        head = (head + 1) % numBuffers;
        while (state_bit[head].state != Available) {
            if (state_bit[head].state == Referenced)
                state_bit[head].state = Available;

            if (num == 2 * numBuffers) {

                throw new BufferPoolExceededException(null, "BUFMGR: BUFFER_EXCEEDED.");

            }
            ++num;
            head = (head + 1) % numBuffers;
        }

        // Make sure pin count is 0.
        /** need to convert assert to a similar function. */
        // assert( (mgr.frameTable())[head].pin_count() == 0 );

        if ((mgr.frameTable())[head].pin_count() != 0) {
            throw new PagePinnedException(null, "BUFMGR: PIN_COUNT IS NOT 0.");
        }

        state_bit[head].state = Pinned;        // Pin this victim so that other
        (mgr.frameTable())[head].pin();
        // process can't pick it as victim (???)

        return head;
    }

    /**
     * Returns the name of the clock algorithm as a string.
     *
     * @return "Clock", the name of the algorithm.
     */
    public final String name() {
        return "Clock";
    }

    /**
     * Displays information from clock replacement algorithm.
     */
    public void info() {
        super.info();
        System.out.println("Clock hand:\t" + head);
        System.out.println("\n\n");
    }

} // end of Clock


// *****************************************************

/**
 * The buffer manager class, it allocates new pages for the
 * buffer pool, pins and unpins the frame, frees the frame
 * page, and uses the replacement algorithm to replace the
 * page.
 */
public class BufMgr implements GlobalConst {

    /**
     * The hash table, only allocated once.
     */
    private BufHashTbl hashTable = new BufHashTbl();

    /**
     * Total number of buffer frames in the buffer pool.
     */
    private int numBuffers;

    /**
     * physical buffer pool.
     */
    private byte[][] bufPool;  // default = byte[NUMBUF][MAX_SPACE];

    /**
     * An array of Descriptors one per frame.
     */
    private FrameDesc[] frmeTable;  // default = new FrameDesc[NUMBUF];

    /**
     * The replacer object, which is only used in this class.
     */
    private Replacer replacer;


    /**
     * Factor out the common code for the two versions of Flush
     *
     * @param pageid    the page number of the page which needs
     *                  to be flushed.
     * @param all_pages the total number of page to be flushed.
     * @throws HashOperationException     if there is a hashtable error.
     * @throws PageUnpinnedException      when unpinning an unpinned page
     * @throws PagePinnedException        when trying to free a pinned page
     * @throws PageNotFoundException      when the page could not be found
     * @throws InvalidPageNumberException when the page number is invalid
     * @throws FileIOException            File I/O  error
     * @throws IOException                Other I/O errors
     */
    private void privFlushPages(PageId pageid, int all_pages)
            throws HashOperationException,
            PageUnpinnedException,
            PagePinnedException,
            PageNotFoundException,
            BufMgrException,
            IOException {
        int i;
        int unpinned = 0;

        for (i = 0; i < numBuffers; i++)   // write all valid dirty pages to disk
            if ((all_pages != 0) || (frmeTable[i].pageNo.pid == pageid.pid)) {

                if (frmeTable[i].pin_count() != 0)
                    unpinned++;

                if (frmeTable[i].dirty != false) {

                    if (frmeTable[i].pageNo.pid == INVALID_PAGE)

                        throw new PageNotFoundException(null, "BUFMGR: INVALID_PAGE_NO");
                    pageid.pid = frmeTable[i].pageNo.pid;


                    Page apage = new Page(bufPool[i]);

                    write_page(pageid, apage);

                    try {
                        hashTable.remove(pageid);
                    } catch (Exception e2) {
                        throw new HashOperationException(e2, "BUFMGR: HASH_TBL_ERROR.");
                    }

                    frmeTable[i].pageNo.pid = INVALID_PAGE; // frame is empty
                    frmeTable[i].dirty = false;
                }

                if (all_pages == 0) {

                    if (unpinned != 0)
                        throw new PagePinnedException(null, "BUFMGR: PAGE_PINNED.");
                }
            }

        if (all_pages != 0) {
            if (unpinned != 0)
                throw new PagePinnedException(null, "BUFMGR: PAGE_PINNED.");
        }
    }


    /**
     * Create a buffer manager object.
     *
     * @param numbufs     number of buffers in the buffer pool.
     * @param replacerArg name of the buffer replacement policy.
     */
    public BufMgr(int numbufs, String replacerArg) {

        numBuffers = numbufs;
        frmeTable = new FrameDesc[numBuffers];
        bufPool = new byte[numBuffers][MAX_SPACE];
        frmeTable = new FrameDesc[numBuffers];

        for (int i = 0; i < numBuffers; i++)  // initialize frameTable
            frmeTable[i] = new FrameDesc();

        if (replacerArg == null) {

            replacer = new Clock(this);

        } else {

            if (replacerArg.compareTo("Clock") == 0) {
                replacer = new Clock(this);
                System.out.println("Replacer: Clock\n");
            } else if (replacerArg.compareTo("LRU") == 0) {
                replacer = new LRU(this);
                System.out.println("Replacer: LRU\n");
            } else if (replacerArg.compareTo("MRU") == 0) {
                replacer = new LRU(this);
                System.out.println("Replacer: MRU\n");
            } else {
                replacer = new Clock(this);
                System.out.println("Replacer:Unknown, Use Clock\n");
            }
        }

        replacer.setBufferManager(this);

    }


    // Debug use only
    private void bmhashdisplay() {
        hashTable.display();
    }


    /**
     * Check if this page is in buffer pool, otherwise
     * find a frame for this page, read in and pin it.
     * Also write out the old page if it's dirty before reading
     * if emptyPage==TRUE, then actually no read is done to bring
     * the page in.
     *
     * @param Page_Id_in_a_DB page number in the minibase.
     * @param page            the pointer poit to the page.
     * @param emptyPage       true (empty page); false (non-empty page)
     * @throws ReplacerException           if there is a replacer error.
     * @throws HashOperationException      if there is a hashtable error.
     * @throws PageUnpinnedException       if there is a page that is already unpinned.
     * @throws InvalidFrameNumberException if there is an invalid frame number .
     * @throws PageNotReadException        if a page cannot be read.
     * @throws BufferPoolExceededException if the buffer pool is full.
     * @throws PagePinnedException         if a page is left pinned .
     * @throws BufMgrException             other error occured in bufmgr layer
     * @throws IOException                 if there is other kinds of I/O error.
     */

    public void pinPage(PageId pin_pgid, Page page, boolean emptyPage)
            throws ReplacerException,
            HashOperationException,
            PageUnpinnedException,
            InvalidFrameNumberException,
            PageNotReadException,
            BufferPoolExceededException,
            PagePinnedException,
            BufMgrException,
            IOException {
        int frameNo;
        boolean bst, bst2;
        PageId oldpageNo = new PageId(-1);
        int needwrite = 0;

        frameNo = hashTable.lookup(pin_pgid);

        if (frameNo < 0) {           // Not in the buffer pool

            frameNo = replacer.pick_victim(); // frameNo is pinned
            if (frameNo < 0) {
                page = null;
                throw new ReplacerException(null, "BUFMGR: REPLACER_ERROR.");

            }

            if ((frmeTable[frameNo].pageNo.pid != INVALID_PAGE)
                    && (frmeTable[frameNo].dirty == true)) {
                needwrite = 1;
                oldpageNo.pid = frmeTable[frameNo].pageNo.pid;
            }

            bst = hashTable.remove(frmeTable[frameNo].pageNo);
            if (bst != true) {
                throw new HashOperationException(null, "BUFMGR: HASH_TABLE_ERROR.");
            }

            frmeTable[frameNo].pageNo.pid = INVALID_PAGE; // frame is empty
            frmeTable[frameNo].dirty = false;             // not dirty

            bst2 = hashTable.insert(pin_pgid, frameNo);

            (frmeTable[frameNo].pageNo).pid = pin_pgid.pid;
            frmeTable[frameNo].dirty = false;

            if (bst2 != true) {
                throw new HashOperationException(null, "BUFMGR: HASH_TABLE_ERROR.");
            }

            Page apage = new Page(bufPool[frameNo]);
            if (needwrite == 1) {
                write_page(oldpageNo, apage);
            } // end of needwrite..

            // read in the page if not empty
            if (emptyPage == false) {
                try {
                    apage.setpage(bufPool[frameNo]);

                    read_page(pin_pgid, apage);
                } catch (Exception e) {


                    bst = hashTable.remove(frmeTable[frameNo].pageNo);
                    if (bst != true)
                        throw new HashOperationException(e, "BUFMGR: HASH_TABLE_ERROR.");

                    frmeTable[frameNo].pageNo.pid = INVALID_PAGE; // frame is empty
                    frmeTable[frameNo].dirty = false;

                    bst = replacer.unpin(frameNo);

                    if (bst != true)
                        throw new ReplacerException(e, "BUFMGR: REPLACER_ERROR.");

                    throw new PageNotReadException(e, "BUFMGR: DB_READ_PAGE_ERROR.");
                }

            }

            page.setpage(bufPool[frameNo]);

            // return true;

        } else {    // the page is in the buffer pool ( frameNo > 0 )

            page.setpage(bufPool[frameNo]);
            replacer.pin(frameNo);

        }
    }

    /**
     * To unpin a page specified by a pageId.
     * If pincount>0, decrement it and if it becomes zero,
     * put it in a group of replacement candidates.
     * if pincount=0 before this call, return error.
     *
     * @param globalPageId_in_a_DB page number in the minibase.
     * @param dirty                the dirty bit of the frame
     * @throws ReplacerException           if there is a replacer error.
     * @throws PageUnpinnedException       if there is a page that is already unpinned.
     * @throws InvalidFrameNumberException if there is an invalid frame number .
     * @throws HashEntryNotFoundException  if there is no entry of page in the hash table.
     */
    public void unpinPage(PageId PageId_in_a_DB, boolean dirty)
            throws ReplacerException,
            PageUnpinnedException,
            HashEntryNotFoundException,
            InvalidFrameNumberException {

        int frameNo;

        frameNo = hashTable.lookup(PageId_in_a_DB);

        if (frameNo < 0) {
            throw new HashEntryNotFoundException(null, "BUFMGR: HASH_NOT_FOUND.");
        }

        if (frmeTable[frameNo].pageNo.pid == INVALID_PAGE) {
            throw new InvalidFrameNumberException(null, "BUFMGR: BAD_FRAMENO.");

        }

        if ((replacer.unpin(frameNo)) != true) {
            throw new ReplacerException(null, "BUFMGR: REPLACER_ERROR.");
        }

        if (dirty == true)
            frmeTable[frameNo].dirty = dirty;

    }


    /**
     * Call DB object to allocate a run of new pages and
     * find a frame in the buffer pool for the first page
     * and pin it. If buffer is full, ask DB to deallocate
     * all these pages and return error (null if error).
     *
     * @param firstpage the address of the first page.
     * @param howmany   total number of allocated new pages.
     * @return the first page id of the new pages.
     * @throws BufferPoolExceededException if the buffer pool is full.
     * @throws HashOperationException      if there is a hashtable error.
     * @throws ReplacerException           if there is a replacer error.
     * @throws HashEntryNotFoundException  if there is no entry of page in the hash table.
     * @throws InvalidFrameNumberException if there is an invalid frame number.
     * @throws PageUnpinnedException       if there is a page that is already unpinned.
     * @throws PagePinnedException         if a page is left pinned.
     * @throws PageNotReadException        if a page cannot be read.
     * @throws IOException                 if there is other kinds of I/O error.
     * @throws BufMgrException             other error occured in bufmgr layer
     * @throws DiskMgrException            other error occured in diskmgr layer
     */
    public PageId newPage(Page firstpage, int howmany)
            throws BufferPoolExceededException,
            HashOperationException,
            ReplacerException,
            HashEntryNotFoundException,
            InvalidFrameNumberException,
            PagePinnedException,
            PageUnpinnedException,
            PageNotReadException,
            BufMgrException,
            DiskMgrException,
            IOException {
        int i;

        PageId firstPageId = new PageId();

        allocate_page(firstPageId, howmany);

        try {
            pinPage(firstPageId, firstpage, true);
        }

        // rollback because pin failed

        catch (Exception e) {
            for (i = 0; i < howmany; i++) {

                firstPageId.pid += i;
                deallocate_page(firstPageId);

            }

            return null;
        }

        return firstPageId;

    }


    /**
     * User should call this method if she needs to delete a page.
     * this routine will call DB to deallocate the page.
     *
     * @param globalPageId the page number in the data base.
     * @throws InvalidBufferException      if buffer pool corrupted.
     * @throws ReplacerException           if there is a replacer error.
     * @throws HashOperationException      if there is a hash table error.
     * @throws InvalidFrameNumberException if there is an invalid frame number.
     * @throws PageNotReadException        if a page cannot be read.
     * @throws BufferPoolExceededException if the buffer pool is already full.
     * @throws PagePinnedException         if a page is left pinned.
     * @throws PageUnpinnedException       if there is a page that is already unpinned.
     * @throws HashEntryNotFoundException  if there is no entry
     *                                     of page in the hash table.
     * @throws IOException                 if there is other kinds of I/O error.
     * @throws BufMgrException             other error occured in bufmgr layer
     * @throws DiskMgrException            other error occured in diskmgr layer
     */
    public void freePage(PageId globalPageId)
            throws InvalidBufferException,
            ReplacerException,
            HashOperationException,
            InvalidFrameNumberException,
            PageNotReadException,
            BufferPoolExceededException,
            PagePinnedException,
            PageUnpinnedException,
            HashEntryNotFoundException,
            BufMgrException,
            DiskMgrException,
            IOException {
        int frameNo;
        frameNo = hashTable.lookup(globalPageId);

        //if globalPageId is not in pool, frameNo < 0
        //then call deallocate
        if (frameNo < 0) {
            deallocate_page(globalPageId);

            return;
        }
        if (frameNo >= (int) numBuffers) {
            throw new InvalidBufferException(null, "BUFMGR, BAD_BUFFER");

        }

        try {
            replacer.free(frameNo);
        } catch (Exception e1) {
            throw new ReplacerException(e1, "BUFMGR, REPLACER_ERROR");
        }

        try {
            hashTable.remove(frmeTable[frameNo].pageNo);
        } catch (Exception e2) {
            throw new HashOperationException(e2, "BUFMGR, HASH_TABLE_ERROR");
        }

        frmeTable[frameNo].pageNo.pid = INVALID_PAGE; // frame is empty
        frmeTable[frameNo].dirty = false;


        deallocate_page(globalPageId);

    }


    /**
     * Added to flush a particular page of the buffer pool to disk
     *
     * @param pageid the page number in the database.
     * @throws HashOperationException if there is a hashtable error.
     * @throws PageUnpinnedException  if there is a page that is already unpinned.
     * @throws PagePinnedException    if a page is left pinned.
     * @throws PageNotFoundException  if a page is not found.
     * @throws BufMgrException        other error occured in bufmgr layer
     * @throws IOException            if there is other kinds of I/O error.
     */
    public void flushPage(PageId pageid)
            throws HashOperationException,
            PageUnpinnedException,
            PagePinnedException,
            PageNotFoundException,
            BufMgrException,
            IOException {
        privFlushPages(pageid, 0);
    }


    /**
     * Flushes all pages of the buffer pool to disk
     *
     * @throws HashOperationException if there is a hashtable error.
     * @throws PageUnpinnedException  if there is a page that is already unpinned.
     * @throws PagePinnedException    if a page is left pinned.
     * @throws PageNotFoundException  if a page is not found.
     * @throws BufMgrException        other error occured in bufmgr layer
     * @throws IOException            if there is other kinds of I/O error.
     */
    public void flushAllPages()
            throws HashOperationException,
            PageUnpinnedException,
            PagePinnedException,
            PageNotFoundException,
            BufMgrException,
            IOException {
        PageId pageId = new PageId(INVALID_PAGE);
        privFlushPages(pageId, 1);
    }


    /**
     * Gets the total number of buffers.
     *
     * @return total number of buffer frames.
     */
    public int getNumBuffers() {
        return numBuffers;
    }


    /**
     * Gets the total number of unpinned buffer frames.
     *
     * @return total number of unpinned buffer frames.
     */
    public int getNumUnpinnedBuffers() {
        return replacer.getNumUnpinnedBuffers();
    }

    /**
     * A few routines currently need direct access to the FrameTable.
     */
    public FrameDesc[] frameTable() {
        return frmeTable;
    }

    private void write_page(PageId pageno, Page page)
            throws BufMgrException {

        try {
            SystemDefs.JavabaseDB.write_page(pageno, page);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: write_page() failed");
        }

    } // end of write_page

    private void read_page(PageId pageno, Page page)
            throws BufMgrException {

        try {
            SystemDefs.JavabaseDB.read_page(pageno, page);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: read_page() failed");
        }

    } // end of read_page

    private void allocate_page(PageId pageno, int num)
            throws BufMgrException {

        try {
            SystemDefs.JavabaseDB.allocate_page(pageno, num);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: allocate_page() failed");
        }

    } // end of allocate_page

    private void deallocate_page(PageId pageno)
            throws BufMgrException {

        try {
            SystemDefs.JavabaseDB.deallocate_page(pageno);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: deallocate_page() failed");
        }

    } // end of deallocate_page

}


/**
 * A class describes the victim data, its frame number and page
 * number.
 */
class victim_data {

    public int frame_num;
    public int page_id;

}
