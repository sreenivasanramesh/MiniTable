/* File hferr.java  */

package heap;

import chainexception.*;

public class HFException extends ChainException {


    public HFException() {
        super();

    }

    public HFException(Exception ex, String name) {
        super(ex, name);
    }


}
