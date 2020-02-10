package catalog;

import chainexception.*;

public class Catalogbadattrcount extends ChainException {

    public Catalogbadattrcount(Exception err, String name) {
        super(err, name);
    }
}

