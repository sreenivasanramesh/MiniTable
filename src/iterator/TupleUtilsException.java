package iterator;

import chainexception.*;

import java.lang.*;

public class TupleUtilsException extends ChainException {
    public TupleUtilsException(String s) {
        super(null, s);
    }

    public TupleUtilsException(Exception prev, String s) {
        super(prev, s);
    }
}
