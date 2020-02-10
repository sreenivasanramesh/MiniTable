package btree;

import chainexception.*;

public class ConstructPageException extends ChainException {
    public ConstructPageException() {
        super();
    }

    public ConstructPageException(String s) {
        super(null, s);
    }

    public ConstructPageException(Exception e, String s) {
        super(e, s);
    }

}
