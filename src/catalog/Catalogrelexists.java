package catalog;

import chainexception.*;

public class Catalogrelexists extends ChainException {

    public Catalogrelexists(Exception err, String name) {
        super(err, name);
    }
}

