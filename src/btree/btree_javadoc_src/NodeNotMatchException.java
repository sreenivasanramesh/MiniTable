package btree;

import chainexception.*;

public class NodeNotMatchException extends ChainException {
    public NodeNotMatchException() {
        super();
    }

    public NodeNotMatchException(String s) {
        super(null, s);
    }

    public NodeNotMatchException(Exception e, String s) {
        super(e, s);
    }

}
