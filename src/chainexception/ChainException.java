/* File ChainException.java  */
package chainexception;

import java.lang.*;
import java.io.*;

/**
 * This class is used for error handling.  Errors are
 * traced from different layer of the minibase system.
 */
public class ChainException extends Exception {
    public Exception prev = null;

    /**
     * Establish the stack for exceptions.
     *
     * @param _prev the previous exception caught
     * @param s     the description for the current exception thrown
     */
    public ChainException(Exception _prev, String s) {
        super(s);
        prev = _prev;
    }


    /**
     * Default constructor.  It will probably never be used
     * since every exception should be pushed onto the stack.
     */
    public ChainException() {
    }

    /**
     * Print out all the exceptions caught across layers.
     */
    public void printStackTrace() {

        if (prev == null) super.printStackTrace();
        if (prev != null) {
            System.out.println(super.toString());
            prev.printStackTrace();
        }

    } // end of printStackTrace

} // end of ChainException
