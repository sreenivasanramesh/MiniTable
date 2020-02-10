package bufmgr;

import chainexception.*;

public class InvalidFrameNumberException extends ChainException {


    public InvalidFrameNumberException(Exception e, String name) {
        super(e, name);
    }


}




