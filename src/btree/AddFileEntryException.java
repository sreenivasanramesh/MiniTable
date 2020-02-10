package btree;

import chainexception.*;

public class AddFileEntryException extends ChainException {
    public AddFileEntryException() {
        super();
    }

    public AddFileEntryException(Exception e, String s) {
        super(e, s);
    }
}
