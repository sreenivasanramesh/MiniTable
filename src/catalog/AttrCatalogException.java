package catalog;

import chainexception.*;

public class AttrCatalogException extends ChainException {

    public AttrCatalogException(Exception err, String name) {
        super(err, name);
    }
}

