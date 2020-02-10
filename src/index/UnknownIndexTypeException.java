package index;

import java.lang.*;

import chainexception.*;

public class UnknownIndexTypeException extends ChainException {
    public UnknownIndexTypeException() {
        super();
    }

    public UnknownIndexTypeException(String s) {
        super(null, s);
    }

    public UnknownIndexTypeException(Exception e, String s) {
        super(e, s);
    }
}
