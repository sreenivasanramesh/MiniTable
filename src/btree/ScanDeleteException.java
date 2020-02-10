package btree;

import chainexception.*;

public class ScanDeleteException extends ChainException {
    public ScanDeleteException() {
        super();
    }

    public ScanDeleteException(String s) {
        super(null, s);
    }

    public ScanDeleteException(Exception e, String s) {
        super(e, s);
    }

}
