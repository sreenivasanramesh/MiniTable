package btree;

import global.*;

/**
 * IndexData: It extends the DataClass.
 * It defines the data "rid" for leaf node in B++ tree.
 */
public class LeafData extends DataClass {
    private RID myRid;

    public String toString() {
        String s;
        s = "[ " + (new Integer(myRid.pageNo.pid)).toString() + " "
                + (new Integer(myRid.slotNo)).toString() + " ]";
        return s;
    }

    /**
     * Class constructor
     *
     * @param rid the data rid
     */
    LeafData(RID rid) {
        myRid = new RID(rid.pageNo, rid.slotNo);
    }

    ;

    /**
     * get a copy of the rid
     *
     * @return the reference of the copy
     */
    public RID getData() {
        return new RID(myRid.pageNo, myRid.slotNo);
    }

    ;

    /**
     * set the rid
     */
    public void setData(RID rid) {
        myRid = new RID(rid.pageNo, rid.slotNo);
    }

    ;
}   
