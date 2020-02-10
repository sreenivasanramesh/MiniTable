package heap;

import chainexception.*;

public class InvalidUpdateException extends ChainException {


    public InvalidUpdateException() {
        super();
    }

    public InvalidUpdateException(Exception ex, String name) {
        super(ex, name);
    }


}
