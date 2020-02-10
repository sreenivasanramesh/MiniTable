package bufmgr;

import chainexception.*;


public class PageNotReadException extends ChainException {


    public PageNotReadException(Exception e, String name) {
        super(e, name);
    }


}




