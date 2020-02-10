package btree;

import chainexception.*;

public class RedistributeException extends ChainException {
    public RedistributeException() {
        super();
    }

    public RedistributeException(String s) {
        super(null, s);
    }

    public RedistributeException(Exception e, String s) {
        super(e, s);
    }

}
