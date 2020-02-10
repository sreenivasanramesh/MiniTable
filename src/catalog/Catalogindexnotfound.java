package catalog;

import chainexception.*;

public class Catalogindexnotfound extends ChainException {

    public Catalogindexnotfound() {
        super();
    }

    public Catalogindexnotfound(Exception err, String name) {
        super(err, name);
    }
}

