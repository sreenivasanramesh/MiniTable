package btree;

import chainexception.*;

public class DeleteRecException extends ChainException {
    public DeleteRecException() {
        super();
    }

    public DeleteRecException(String s) {
        super(null, s);
    }

    public DeleteRecException(Exception e, String s) {
        super(e, s);
    }

}
