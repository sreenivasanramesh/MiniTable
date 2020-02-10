package diskmgr;

import chainexception.*;

public class DuplicateEntryException extends ChainException {

    public DuplicateEntryException(Exception e, String name) {
        super(e, name);
    }
}

