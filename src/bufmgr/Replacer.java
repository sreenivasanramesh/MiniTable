package bufmgr;

import global.*;

/**
 * This interface assocaites with the replace algorithm,
 * it describs if a buffer frame page is pinned, unpinned,
 * or available.
 */
class STATE {

    int state;
    //  Available = 12;
    //  Referenced = 13;
    //  Pinned = 14;
}


/**
 * A super class for buffer pool replacement algorithm. It describes
 * which frame to be picked up for replacement by a certain replace
 * algorithm.
 */
abstract class Replacer implements GlobalConst {

    /**
     * Pins a candidate page in the buffer pool.
     *
     * @param frameNo frame number of the page.
     * @return true if successful.
     * @throws InvalidFrameNumberException if the frame number is less than zero
     *                                     or bigger than number of buffers.
     */
    public void pin(int frameNo) throws InvalidFrameNumberException {

        if ((frameNo < 0) || (frameNo >= (int) mgr.getNumBuffers())) {

            throw new InvalidFrameNumberException(null, "BUFMGR: BAD_BUFFRAMENO.");
        }


        (mgr.frameTable())[frameNo].pin();
        state_bit[frameNo].state = Pinned;
    }

    /**
     * Unpins a page in the buffer pool.
     *
     * @param frameNo frame number of the page.
     * @return true if successful.
     * @throws InvalidFrameNumberException if the frame number is less than zero
     *                                     or bigger than number of buffers.
     * @throws PageUnpinnedException       if the page is originally unpinned.
     */
    public boolean unpin(int frameNo) throws InvalidFrameNumberException, PageUnpinnedException {
        if ((frameNo < 0) || (frameNo >= (int) mgr.getNumBuffers())) {

            throw new InvalidFrameNumberException(null, "BUFMGR: BAD_BUFFRAMENO.");

        }

        if ((mgr.frameTable())[frameNo].pin_count() == 0) {

            throw new PageUnpinnedException(null, "BUFMGR: PAGE_NOT_PINNED.");

        }

        (mgr.frameTable())[frameNo].unpin();

        if ((mgr.frameTable())[frameNo].pin_count() == 0)
            state_bit[frameNo].state = Referenced;
        return true;

    }


    /**
     * Frees and unpins a page in the buffer pool.
     *
     * @param frameNo frame number of the page.
     * @throws PagePinnedException if the page is pinned.
     */
    public void free(int frameNo) throws PagePinnedException {

        if ((mgr.frameTable())[frameNo].pin_count() > 1) {

            throw new PagePinnedException(null, "BUFMGR: PAGE_PINNED.");

        }

        (mgr.frameTable())[frameNo].unpin();
        state_bit[frameNo].state = Available;

    }


    /**
     * Must pin the returned frame.
     */
    public abstract int pick_victim() throws BufferPoolExceededException, PagePinnedException;

    /**
     * Retruns the name of the replacer algorithm.
     */
    public abstract String name();

    /**
     * Prints out the information of the buffer frame.
     */
    public void info() {

        System.out.println("\nInfo:\nstate_bits:(R)eferenced | (A)vailable | (P)inned");

        int numBuffers = mgr.getNumBuffers();

        for (int i = 0; i < numBuffers; ++i) {
            if (((i + 1) % 9) == 0)
                System.out.println("\n");
            System.out.println("(" + i + ") ");
            switch (state_bit[i].state) {
                case Referenced:
                    System.out.println("R\t");
                    break;
                case Available:
                    System.out.println("A\t");
                    break;
                case Pinned:
                    System.out.println("P\t");
                    break;
                default:
                    System.err.println("ERROR from Replacer.info()");
                    break;
            }
        }

        System.out.println("\n\n");

    }

    /**
     * Counts the unpinned frames (free frames) in the
     * buffer pool.
     *
     * @returns the total number of unpinned frames in
     * the buffer pool.
     */
    public int getNumUnpinnedBuffers() {
        int numBuffers = mgr.getNumBuffers();
        int answer = 0;
        for (int index = 0; index < numBuffers; ++index)
            if ((mgr.frameTable())[index].pin_count() == 0)
                ++answer;

        return answer;
    }

    /**
     * Creates a replacer object.
     */
    protected Replacer(BufMgr javamgr) {
        mgr = javamgr;
        int numbuf = javamgr.getNumBuffers();
        state_bit = new STATE[numbuf];
        for (int i = 0; i < numbuf; i++)
            state_bit[i] = new STATE();
        head = -1;
    }

    /**
     * A buffer manager object.
     */
    protected BufMgr mgr;

    /**
     * Sets the buffer manager to be eqaul to the buffer manager
     * in the argument, gets the total number of buffer frames,
     * and mainstains the head of the clock.
     *
     * @param mgr the buffer manage to be assigned to.
     */
    protected void setBufferManager(BufMgr mgrArg) {

        mgr = mgrArg;
        int numBuffers = mgr.getNumBuffers();


        // These are the C++ code which we ignored
        //char *Sh_StateArr = MINIBASE_SHMEM->malloc((numBuffers * sizeof(STATE)));
        //state_bit = new(Sh_StateArr) STATE[numBuffers];


        for (int index = 0; index < numBuffers; ++index) {
            state_bit[index].state = Available;
        }


        head = -1; // maintain the head of the clock.
    }


    /** These variables are required for the clock algorithm. */

    /**
     * Clock hand.
     */
    protected int head;

    /**
     * The state of a frame.
     */
    protected STATE state_bit[];

    public static final int Available = 12;
    public static final int Referenced = 13;
    public static final int Pinned = 14;
}


