package index;

import java.lang.*;

import chainexception.*;

public class InvalidSelectionException extends ChainException {
    public InvalidSelectionException() {
        super();
    }

    public InvalidSelectionException(String s) {
        super(null, s);
    }

    public InvalidSelectionException(Exception e, String s) {
        super(e, s);
    }
}
