package bufmgr;

import chainexception.*;

public class PageUnpinnedException extends ChainException {

    public PageUnpinnedException(Exception ex, String name) {
        super(ex, name);
    }


}




