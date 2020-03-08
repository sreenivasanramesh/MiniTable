package global;

import java.io.IOException;

public class MID {
    private int slotNo;
    private PageId pageNo = new PageId();

    public MID(PageId pageNo, int slotNo) {
        this.slotNo = slotNo;
        this.pageNo = pageNo;
    }

    public MID() {
    }

    public void copyMid(MID mid) {
        this.slotNo = mid.slotNo;
        this.pageNo = mid.pageNo;
    }

    public void writeTOByteArray(byte[] array, int offset) throws IOException {
        Convert.setIntValue(slotNo, offset, array);
        Convert.setIntValue(pageNo.pid, offset + 4, array);
    }

    public boolean equals(MID mid) {
        return this.pageNo.pid == mid.pageNo.pid && this.slotNo == mid.slotNo;
    }

}