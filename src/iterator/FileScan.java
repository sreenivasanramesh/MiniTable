package iterator;


import BigT.InvalidStringSizeArrayException;
import BigT.Map;
import bufmgr.PageNotReadException;
import cmdline.MiniTable;
import global.AttrType;
import global.MID;
import heap.*;

import java.io.IOException;

/**
 * open a heapfile and according to the condition expression to get
 * output file, call get_next to get all tuples
 */
public class FileScan extends MapIterator {
    private AttrType[] _in1;
    private short in1_len;
    private short[] s_sizes;
    private Heapfile f;
    private MapScan scan;
    //private Scan scan;
    //private Tuple tuple1;
    private Map mapObj;
    private Map tempMap;
    //private Tuple Jtuple;
    private int t1_size;
    private int nOutFlds;
    private CondExpr[] OutputFilter;
    public FldSpec[] perm_mat;


    /**
     * constructor
     *
     * @param file_name  heapfile to be opened
     * @param in1        array showing what the attributes of the input fields are.
     * @param s1_sizes   shows the length of the string fields.
     * @param len_in1    number of attributes in the input tuple
     * @param n_out_flds number of fields in the out tuple
     * @param proj_list  shows what input fields go where in the output tuple
     * @param outFilter  select expressions
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    public FileScan(String file_name,
                    AttrType[] in1,
                    short[] s1_sizes,
                    short len_in1,
                    int n_out_flds,
                    FldSpec[] proj_list,
                    CondExpr[] outFilter
    )
            throws IOException,
            FileScanException,
            TupleUtilsException,
            InvalidRelation {
        _in1 = in1;
        in1_len = len_in1;
        s_sizes = s1_sizes;

        mapObj = new Map();
        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] ts_size;
        ts_size = MapUtils.setup_op_tuple(mapObj, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);

        OutputFilter = outFilter;
        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        tempMap = new Map();

        try {
            tempMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
        } catch (Exception e) {
            throw new FileScanException(e, "setHdr() failed");
        }
        t1_size = tempMap.size();

        try {
            f = new Heapfile(file_name);

        } catch (Exception e) {
            throw new FileScanException(e, "Create new heapfile failed");
        }

        try {
            scan = f.openMapScan();
        } catch (Exception e) {
            throw new FileScanException(e, "openScan() failed");
        }
    }

    /**
     * @return shows what input fields go where in the output tuple
     */
    public FldSpec[] show() {
        return perm_mat;
    }

    /**
     * @return the result tuple
     * @throws JoinsException                 some join exception
     * @throws IOException                    I/O errors
     * @throws InvalidTupleSizeException      invalid tuple size
     * @throws InvalidTypeException           tuple type not valid
     * @throws PageNotReadException           exception from lower layer
     * @throws PredEvalException              exception from PredEval class
     * @throws UnknowAttrType                 attribute type unknown
     * @throws FieldNumberOutOfBoundException array out of bounds
     * @throws WrongPermat                    exception for wrong FldSpec argument
     */
    public Map get_next()
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat, InvalidMapSizeException, InvalidStringSizeArrayException {
        MID mid = new MID();

        while (true) {
            if ((tempMap = scan.getNext(mid)) == null) {
                return null;
            }
//            return tempMap;
            tempMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
            if (PredEval.Eval(OutputFilter, tempMap, null, _in1, null) == true) {
                Projection.Project(tempMap, _in1, mapObj, perm_mat);
                return mapObj;
            }
        }
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     */
    public void close() {

        if (!closeFlag) {
            scan.closescan();
            closeFlag = true;
        }
    }

}


