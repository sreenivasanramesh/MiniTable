package catalog;

import chainexception.*;

public class Catalogdupattrs extends ChainException {

    public Catalogdupattrs(Exception err, String name) {
        super(err, name);
    }
}

