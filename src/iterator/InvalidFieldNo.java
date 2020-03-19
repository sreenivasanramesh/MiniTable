package iterator;

import chainexception.ChainException;

public class InvalidFieldNo extends ChainException {
    public InvalidFieldNo(String s) { super(null, s);
    }
}