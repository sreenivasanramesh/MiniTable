package bufmgr;

import chainexception.*;

public class ReplacerException extends ChainException {

    public ReplacerException(Exception e, String name) {
        super(e, name);
    }

}

