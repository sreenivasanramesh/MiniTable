package catalog;

import chainexception.*;

public class Catalogrelnotfound extends ChainException {

    public Catalogrelnotfound(Exception err, String name) {
        super(err, name);
    }
}

