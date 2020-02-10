package diskmgr;

import chainexception.*;

public class InvalidPageNumberException extends ChainException {


    public InvalidPageNumberException(Exception ex, String name) {
        super(ex, name);
    }
}




