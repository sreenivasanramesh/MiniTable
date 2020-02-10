package btree;

import chainexception.*;

public class InsertRecException extends ChainException {
    public InsertRecException() {
        super();
    }

    public InsertRecException(String s) {
        super(null, s);
    }

    public InsertRecException(Exception e, String s) {
        super(e, s);
    }

}
