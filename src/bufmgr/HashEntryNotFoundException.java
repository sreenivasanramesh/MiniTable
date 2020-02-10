package bufmgr;

import chainexception.*;


public class HashEntryNotFoundException extends ChainException {

    public HashEntryNotFoundException(Exception ex, String name) {
        super(ex, name);
    }


}




