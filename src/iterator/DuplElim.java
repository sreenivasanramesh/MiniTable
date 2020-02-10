package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.io.*;

/**
 * Eleminate the duplicate tuples from the input relation
 */
public class DuplElim extends Iterator {
    private AttrType[] _in;     // memory for array allocated by constructor
    private short in_len;
    private short[] str_lens;

    private Iterator _am;
    private boolean done;

    private AttrType sortFldType;
    private int sortFldLen;
    private Tuple Jtuple;

    private Tuple TempTuple1, TempTuple2;

    /**
     * Constructor to set up some information.
     *
     * @param in[]       Array containing field types of R.
     * @param len_in     # of columns in R.
     * @param s_sizes[]  store the length of string appeared in tuple
     * @param am         input relation iterator, access method for left input to join,
     * @param amt_of_mem the page numbers required IN PAGES
     * @throws IOException       some I/O fault
     * @throws DuplElimException the exception from DuplElim.java
     */
    public DuplElim(
            AttrType in[],
            short len_in,
            short s_sizes[],
            Iterator am,
            int amt_of_mem,
            boolean inp_sorted
    ) throws IOException, DuplElimException {
        _in = new AttrType[in.length];
        System.arraycopy(in, 0, _in, 0, in.length);
        in_len = len_in;

        Jtuple = new Tuple();
        try {
            Jtuple.setHdr(len_in, _in, s_sizes);
        } catch (Exception e) {
            throw new DuplElimException(e, "setHdr() failed");
        }

        sortFldType = in[0];
        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                sortFldLen = 4;
                break;
            case AttrType.attrReal:
                sortFldLen = 4;
                break;
            case AttrType.attrString:
                sortFldLen = s_sizes[0];
                break;
            default:
                //error("Unknown type");
                return;
        }

        _am = am;
        TupleOrder order = new TupleOrder(TupleOrder.Ascending);
        if (!inp_sorted) {
            try {
                _am = new Sort(in, len_in, s_sizes, am, 1, order,
                        sortFldLen, amt_of_mem);
            } catch (SortException e) {
                e.printStackTrace();
                throw new DuplElimException(e, "SortException is caught by DuplElim.java");
            }
        }

        // Allocate memory for the temporary tuples
        TempTuple1 = new Tuple();
        TempTuple2 = new Tuple();
        try {
            TempTuple1.setHdr(in_len, _in, s_sizes);
            TempTuple2.setHdr(in_len, _in, s_sizes);
        } catch (Exception e) {
            throw new DuplElimException(e, "setHdr() failed");
        }
        done = false;
    }

    /**
     * The tuple is returned.
     *
     * @return call this function to get the tuple
     * @throws JoinsException            some join exception
     * @throws IndexException            exception from super class
     * @throws IOException               I/O errors
     * @throws InvalidTupleSizeException invalid tuple size
     * @throws InvalidTypeException      tuple type not valid
     * @throws PageNotReadException      exception from lower layer
     * @throws TupleUtilsException       exception from using tuple utilities
     * @throws PredEvalException         exception from PredEval class
     * @throws SortException             sort exception
     * @throws LowMemException           memory error
     * @throws UnknowAttrType            attribute type unknown
     * @throws UnknownKeyTypeException   key type unknown
     * @throws Exception                 other exceptions
     */
    public Tuple get_next()
            throws IOException,
            JoinsException,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception {
        Tuple t;

        if (done)
            return null;
        Jtuple.tupleCopy(TempTuple1);

        do {
            if ((t = _am.get_next()) == null) {
                done = true;                    // next call returns DONE;
                return null;
            }
            TempTuple2.tupleCopy(t);
        } while (TupleUtils.Equal(TempTuple1, TempTuple2, _in, in_len));

        // Now copy the the TempTuple2 (new o/p tuple) into TempTuple1.
        TempTuple1.tupleCopy(TempTuple2);
        Jtuple.tupleCopy(TempTuple2);
        return Jtuple;
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     *
     * @throws JoinsException join error from lower layers
     */
    public void close() throws JoinsException {
        if (!closeFlag) {

            try {
                _am.close();
            } catch (Exception e) {
                throw new JoinsException(e, "DuplElim.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
