package diskmgr;

import chainexception.*;


public class FileIOException extends ChainException {

    public FileIOException(Exception e, String name) {
        super(e, name);
    }


}




