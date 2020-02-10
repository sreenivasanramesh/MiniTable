package btree;

import chainexception.*;

public class IndexSearchException extends ChainException {
    public IndexSearchException() {
        super();
    }

    public IndexSearchException(String s) {
        super(null, s);
    }

    public IndexSearchException(Exception e, String s) {
        super(e, s);
    }

}
