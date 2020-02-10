package catalog;

import chainexception.*;

public class CatalogException extends ChainException {

    public CatalogException(Exception err, String name) {
        super(err, name);
    }
}

