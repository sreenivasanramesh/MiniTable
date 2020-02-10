package btree;

import chainexception.*;

public class PinPageException extends ChainException {
    public PinPageException() {
        super();
    }

    public PinPageException(String s) {
        super(null, s);
    }

    public PinPageException(Exception e, String s) {
        super(e, s);
    }

}
