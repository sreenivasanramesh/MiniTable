package heap;

import chainexception.*;

public class FileAlreadyDeletedException extends ChainException {

    public FileAlreadyDeletedException() {
        super();
    }

    public FileAlreadyDeletedException(Exception ex, String name) {
        super(ex, name);
    }

}
