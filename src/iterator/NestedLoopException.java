package iterator;

import chainexception.*;

import java.lang.*;

public class NestedLoopException extends ChainException {
    public NestedLoopException(String s) {
        super(null, s);
    }

    public NestedLoopException(Exception prev, String s) {
        super(prev, s);
    }
}
