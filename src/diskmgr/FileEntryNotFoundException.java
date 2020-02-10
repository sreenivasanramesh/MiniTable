package diskmgr;

import chainexception.*;


public class FileEntryNotFoundException extends ChainException {

    public FileEntryNotFoundException(Exception e, String name) {
        super(e, name);
    }


}




