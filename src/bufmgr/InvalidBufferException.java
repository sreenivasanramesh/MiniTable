package bufmgr;

import chainexception.*;

public class InvalidBufferException extends ChainException {


    public InvalidBufferException(Exception e, String name) {
        super(e, name);
    }

}




