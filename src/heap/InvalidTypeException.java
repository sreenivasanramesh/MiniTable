package heap;

import chainexception.*;


public class InvalidTypeException extends ChainException {


    public InvalidTypeException() {
        super();
    }

    public InvalidTypeException(Exception ex, String name) {
        super(ex, name);
    }


}
