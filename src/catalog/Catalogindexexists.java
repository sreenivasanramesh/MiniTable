package catalog;

import chainexception.*;

public class Catalogindexexists extends ChainException {

    public Catalogindexexists() {
        super();
    }

    public Catalogindexexists(Exception err, String name) {
        super(err, name);
    }
}

