/* File MRU.java */

package bufmgr;

import diskmgr.*;
import global.*;

/**
 * class MRU is a subclass of class Replacer using MRU
 * algorithm for page replacement
 */
class MRU extends Replacer {

    /* private fields and methods */

    /**
     * private field
     * An array to hold number of frames in the buffer pool
     */
    private int frames[];


    /**
     * Calling super class the same method
     * Initializing the frames[] with number of buffer allocated
     * by buffer manager
     * set each element of frame[] < 0
     *
     * @param mgr Buffer manager
     * @see BufMgr
     * @see Replacer
     */

    public void setBufferManager(BufMgr mgr) {
        super.setBufferManager(mgr);

        int numBuffers = mgr.getNumBuffers();
        frames = new int[numBuffers];

        for (int index = 0; index < numBuffers; ++index)
            frames[index] = -index;

        frames[0] = -numBuffers;
    }

    /**
     * Class constructor
     * Initializing frames[] pointer = null.
     */

    public MRU(BufMgr mgrArg) {
        super(mgrArg);
        frames = null;

    }


    /**
     * Adding the frame with given frame number to buffer pool
     * putting it in front of the list
     *
     * @param    frameNo     the frame number
     * @see BufMgr
     */

    private void update(int frameNo) {
        int index;
        int numBuffers = mgr.getNumBuffers();
        for (index = 0; index < numBuffers; ++index)
            if (frames[index] < 0 || frames[index] == frameNo)
                break;


        // If buffer pool is not yet full, add this frame to it...
        if (frames[index] < 0)
            frames[index] = frameNo;

        int frame = frames[index];
        while (index-- > 0)
            frames[index + 1] = frames[index];

        frames[0] = frame;
    }

    /**
     * pin the page with the given frame number
     * update the buffer pool
     *
     * @param frameNo the frame number to pin
     * @@exception InvalidFrameNumberException
     */

    public void pin(int frameNo) throws InvalidFrameNumberException {
        super.pin(frameNo);

        update(frameNo);
    }

    /**
     * Finding a free frame in the buffer pool
     * or choosing a page to replace using MRU policy
     * Update the buffer pool
     *
     * @return return the frame number
     * return -1 if No victims found
     */


    public int pick_victim() {
        int numBuffers = mgr.getNumBuffers();
        int i, frame;

        for (i = 0; i < numBuffers; ++i)
            if (frames[i] < 0) {
                if (i == 0)
                    frames[i] = 0;
                else
                    frames[i] *= -1;
                frame = frames[i];
                state_bit[frame].state = Pinned;
                (mgr.frameTable())[frame].pin();
                update(frame);
                return frame;
            }

        for (i = 0; i < numBuffers; ++i) {
            frame = frames[i];
            if (state_bit[frame].state != Pinned) {
                state_bit[frame].state = Pinned;
                (mgr.frameTable())[frame].pin();
                update(frame);
                return frame;
            }
        }

        return -1;   // No victims found!!
    }

    /**
     * get the page replacement policy name
     *
     * @return return the name of replacement policy used
     */

    public String name() {
        return "MRU";
    }

    /**
     * print out the information of frame usage
     */

    public void info() {
        super.info();

        System.out.print("MRU REPLACEMENT");

        for (int i = 0; i < mgr.getNumBuffers(); i++) {
            if (i % 5 == 0)
                System.out.println();
            System.out.print("\t" + frames[i]);

        }
        System.out.println();
    }

}
