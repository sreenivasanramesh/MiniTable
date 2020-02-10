package btree;

import chainexception.*;

public class ConvertException extends ChainException {
    public ConvertException() {
        super();
    }

    public ConvertException(String s) {
        super(null, s);
    }

    public ConvertException(Exception e, String s) {
        super(e, s);
    }
}
