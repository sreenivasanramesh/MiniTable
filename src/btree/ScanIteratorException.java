package btree;

import chainexception.*;

public class ScanIteratorException extends ChainException {
    public ScanIteratorException() {
        super();
    }

    public ScanIteratorException(String s) {
        super(null, s);
    }

    public ScanIteratorException(Exception e, String s) {
        super(e, s);
    }

}
