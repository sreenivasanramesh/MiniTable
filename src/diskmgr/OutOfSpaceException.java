package diskmgr;

import chainexception.*;

public class OutOfSpaceException extends ChainException {

    public OutOfSpaceException(Exception e, String name) {
        super(e, name);
    }
}

