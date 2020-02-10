package btree;

import chainexception.*;

public class IndexInsertRecException extends ChainException {
    public IndexInsertRecException() {
        super();
    }

    public IndexInsertRecException(String s) {
        super(null, s);
    }

    public IndexInsertRecException(Exception e, String s) {
        super(e, s);
    }

}
