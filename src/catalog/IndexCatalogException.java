package catalog;

import chainexception.*;

public class IndexCatalogException extends ChainException {

    public IndexCatalogException(Exception err, String name) {
        super(err, name);
    }
}

