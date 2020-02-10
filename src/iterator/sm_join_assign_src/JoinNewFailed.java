package iterator;

import java.lang.*;

import chainexception.*;

public class JoinNewFailed extends ChainException {
    public JoinNewFailed(String s) {
        super(null, s);
    }

    public JoinNewFailed(Exception prev, String s) {
        super(prev, s);
    }
}
