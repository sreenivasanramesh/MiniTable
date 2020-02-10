package btree;

import chainexception.*;

public class DeleteFashionException extends ChainException {
    public DeleteFashionException() {
        super();
    }

    public DeleteFashionException(String s) {
        super(null, s);
    }

    public DeleteFashionException(Exception e, String s) {
        super(e, s);
    }

}
