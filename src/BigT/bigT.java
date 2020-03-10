package BigT;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import global.PageId;
import global.MID;
import global.AttrType;
import BigT.Stream;

import java.io.IOException;

public class bigT {

    private int type;
    private Heapfile heapfile;



    // Initialize the big table.typeis an integer be-tween 1 and 5 and the different types will correspond to different clustering and indexing strategies youwill use for the bigtable.
    void bigT(String name, int type) throws HFException, IOException, HFDiskMgrException, InvalidPageNumberException, DiskMgrException, FileIOException, HFBufMgrException {
        this.type = type;
        Stream stream;
        MID mid;

        try {
            PageId pageid = SystemDefs.JavabaseDB.get_file_entry(name + ".hfile");
            if (pageid == null) {
                throw new Exception("Heap file does not exist");
            }
            //create a new object fot the heap file
            this.heapfile = new Heapfile(name + ".hfile");

            //TODO: this needs to be added in heap file to work with map representation
            // ie it should return a stream object
            stream = heapfile.openStream();
            mid = new MID();
            //TODO: get next with MID has to be added to Stream.java
            Map header = stream.getNext(Mid mid);
            //TODO: need to update setHeader

            AttrType[] atype = new AttrType[] {new AttrType(0),
                                               new AttrType(0),
                                               new AttrType(1),
                                               new AttrType(0)};
            header.setHeader((short) 4, atype);






        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    //Delete the bigtable from the database.
    void deleteBigt() {

    }

    // Return number of maps in the bigtable.
    int getMapCnt() {

        return 0;
    }

    // Return number of distinct row labels in the bigtable.
    int getRowCnt() {
        return 0;
    }

    //    Return number of distinct column labels in the bigtable.
    int getColumnCnt() {

        return 0;
    }

    // TODO: insert and return MID
    MID insertMap(byte[] mapPtr) {
        return new MID();
    }

}