package bufmgr;

import chainexception.*;

public class BufMgrException extends ChainException {

    public BufMgrException(Exception e, String name) {
        super(e, name);
    }

}

