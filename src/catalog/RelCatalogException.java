package catalog;

import chainexception.*;

public class RelCatalogException extends ChainException {

    public RelCatalogException(Exception err, String name) {
        super(err, name);
    }
}

