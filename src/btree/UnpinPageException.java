package btree;

import chainexception.*;

public class UnpinPageException extends ChainException {
    public UnpinPageException() {
        super();
    }

    public UnpinPageException(String s) {
        super();
    }

    public UnpinPageException(Exception e, String s) {
        super(e, s);
    }

}
