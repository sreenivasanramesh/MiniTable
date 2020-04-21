package iterator;

import BigT.Map;
import cmdline.MiniTable;
import heap.*;
import global.*;
import bufmgr.*;
import index.*;

import java.io.*;

/**
 * This file contains the interface for the sort_merge joins.
 * We name the two relations being joined as R and S.
 * This file contains an implementation of the sort merge join
 * algorithm as described in the Shapiro paper. It makes use of the external
 * sorting utility to generate runs, and then uses the iterator interface to
 * get successive tuples for the final merge.
 */
public class SortMerge extends MapIterator implements GlobalConst {
    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private MapIterator p_i1,        // pointers to the two iterators. If the
            p_i2;               // inputs are sorted, then no sorting is done
    private TupleOrder _order;                      // The sorting order.
    private CondExpr OutputFilter[];

    private boolean get_from_in1, get_from_in2;        // state variables for get_next
    private int jc_in1, jc_in2;
    private boolean process_next_block;
    private short inner_str_sizes[];
    private IoBuf io_buf1, io_buf2;
    private Map TempMap1, TempMap2;
    private Map map1, map2;
    private boolean done;
    private byte _bufs1[][], _bufs2[][];
    private int _n_pages;
    private Heapfile temp_file_fd1, temp_file_fd2;
    private AttrType sortFldType;
    private int t1_size, t2_size;
    private Map JMap;
    private FldSpec perm_mat[];
    private int nOutFlds;

    /**
     * constructor,initialization
     *
     * @param in1        Array containing field types of R
     * @param len_in1      # of columns in R
     * @param s1_sizes     shows the length of the string fields in R.
     * @param in2        Array containing field types of S
     * @param len_in2      # of columns in S
     * @param s2_sizes     shows the length of the string fields in S
     * @param sortFld1Len  the length of sorted field in R
     * @param sortFld2Len  the length of sorted field in S
     * @param join_col_in1 The col of R to be joined with S
     * @param join_col_in2 the col of S to be joined with R
     * @param amt_of_mem   IN PAGES
     * @param am1          access method for left input to join
     * @param am2          access method for right input to join
     * @param in1_sorted   is am1 sorted?
     * @param in2_sorted   is am2 sorted?
     * @param order        the order of the tuple: assending or desecnding?
     * @param outFilter  Ptr to the output filter
     * @param proj_list    shows what input fields go where in the output tuple
     * @param n_out_flds   number of outer relation fileds
     * @throws JoinNewFailed       allocate failed
     * @throws JoinLowMemory       memory not enough
     * @throws SortException       exception from sorting
     * @throws TupleUtilsException exception from using tuple utils
     * @throws IOException         some I/O fault
     */
    public SortMerge(AttrType in1[],
                     int len_in1,
                     short s1_sizes[],
                     AttrType in2[],
                     int len_in2,
                     short s2_sizes[],

                     int join_col_in1,
                     int sortFld1Len,
                     int join_col_in2,
                     int sortFld2Len,

                     int amt_of_mem,
                     MapIterator am1,
                     MapIterator am2,

                     boolean in1_sorted,
                     boolean in2_sorted,
                     TupleOrder order,

                     CondExpr outFilter[],
                     FldSpec proj_list[],
                     int n_out_flds
    )
            throws JoinNewFailed,
            JoinLowMemory,
            SortException,
            TupleUtilsException,
            IOException {
        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        JMap = new Map();
        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] ts_size = null;
        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            ts_size = MapUtils.setup_op_tuple(JMap, Jtypes, in1, len_in1, s1_sizes, proj_list,n_out_flds);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "Exception is caught by SortMerge.java");
        }

        int n_strs2 = 0;

        for (int i = 0; i < len_in2; i++) if (_in2[i].attrType == AttrType.attrString) n_strs2++;
        inner_str_sizes = new short[n_strs2];

        for (int i = 0; i < n_strs2; i++) inner_str_sizes[i] = s2_sizes[i];

        p_i1 = am1;
        p_i2 = am2;

        if (!in1_sorted) {
            try {
                p_i1 = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, am1, join_col_in1, order, amt_of_mem / 2, sortFld1Len);
        } catch (Exception e) {
                throw new SortException(e, "Sort failed");
            }
        }

        if (!in2_sorted) {
            try {
                p_i2 = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, am2, join_col_in2, order, amt_of_mem/2, sortFld2Len);
            } catch (Exception e) {
                throw new SortException(e, "Sort failed");
            }
        }

        OutputFilter = outFilter;
        _order = order;
        jc_in1 = join_col_in1;
        jc_in2 = join_col_in2;
        get_from_in1 = true;
        get_from_in2 = true;

        // open io_bufs
        io_buf1 = new IoBuf();
        io_buf2 = new IoBuf();

        // Allocate memory for the temporary tuples
        TempMap1 = new Map();
        TempMap2 = new Map();
        map1 = new Map();
        map2 = new Map();


        if (io_buf1 == null || io_buf2 == null ||
                TempMap1 == null || TempMap2 == null ||
                map1 == null || map2 == null)
            throw new JoinNewFailed("SortMerge.java: allocate failed");
        if (amt_of_mem < 2)
            throw new JoinLowMemory("SortMerge.java: memory not enough");

        try {
            TempMap1.setHeader( _in1, s1_sizes);
            map1.setHeader(_in1, s1_sizes);
            TempMap2.setHeader(_in2, s2_sizes);
            map2.setHeader(_in2, s2_sizes);
        } catch (Exception e) {
            throw new SortException(e, "Set header failed");
        }
        t1_size = map1.size();
        t2_size = map2.size();

        process_next_block = true;
        done = false;

        // Two buffer pages to store equivalence classes
        // NOTE -- THESE PAGES ARE NOT OBTAINED FROM THE BUFFER POOL
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        _n_pages = 1;
        _bufs1 = new byte[_n_pages][MINIBASE_PAGESIZE];
        _bufs2 = new byte[_n_pages][MINIBASE_PAGESIZE];


        temp_file_fd1 = null;
        temp_file_fd2 = null;
        try {
            temp_file_fd1 = new Heapfile(null);
            temp_file_fd2 = new Heapfile(null);

        } catch (Exception e) {
            throw new SortException(e, "Create heap file failed");
        }

        sortFldType = _in1[jc_in1 - 1];

        // Now, that stuff is setup, all we have to do is a get_next !!!!
    }

    /**
     * The tuple is returned
     * All this function has to do is to get 1 tuple from one of the Iterators
     * (from both initially), use the sorting order to determine which one
     * gets sent up. Amit)
     * Hmmm it seems that some thing more has to be done in order to account
     * for duplicates.... => I am following Raghu's 564 notes in order to
     * obtain an algorithm for this merging. Some funda about
     * "equivalence classes"
     *
     * @return the joined tuple is returned
     * @throws IOException               I/O errors
     * @throws JoinsException            some join exception
     * @throws IndexException            exception from super class
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

    public Map get_next()
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

        int comp_res;
        Map _map1, _map2;
        if (done) return null;

        while (true) {
            if (process_next_block) {
                process_next_block = false;
                if (get_from_in1)
                    if ((map1 = p_i1.get_next()) == null) {
                        done = true;
                        return null;
                    }
                if (get_from_in2)
                    if ((map2 = p_i2.get_next()) == null) {
                        done = true;
                        return null;
                    }
                get_from_in1 = get_from_in2 = false;

                // Note that depending on whether the sort order
                // is ascending or descending,
                // this loop will be modified.
                comp_res = MapUtils.CompareMapWithMap(map1, map2, jc_in1);
                while ((comp_res < 0 && _order.tupleOrder == TupleOrder.Ascending) ||
                        (comp_res > 0 && _order.tupleOrder == TupleOrder.Descending)) {
                    if ((map1 = p_i1.get_next()) == null) {
                        done = true;
                        return null;
                    }

                    comp_res =  MapUtils.CompareMapWithMap(map1, map2, jc_in1);
                }

                comp_res =  MapUtils.CompareMapWithMap(map1, map2, jc_in1);
                while ((comp_res > 0 && _order.tupleOrder == TupleOrder.Ascending) ||
                        (comp_res < 0 && _order.tupleOrder == TupleOrder.Descending)) {
                    if ((map2 = p_i2.get_next()) == null) {
                        done = true;
                        return null;
                    }
                    comp_res =  MapUtils.CompareMapWithMap(map1, map2, jc_in1);
                }

                if (comp_res != 0) {
                    process_next_block = true;
                    continue;
                }

                TempMap1.copyMap(map1);
                TempMap2.copyMap(map2);

                io_buf1.init(_bufs1, 1, t1_size, temp_file_fd1);
                io_buf2.init(_bufs2, 1, t2_size, temp_file_fd2);

                while (MapUtils.CompareMapWithMap(map1, TempMap1, jc_in1) == 0) {
                    // Insert tuple1 into io_buf1
                    try {
                        io_buf1.Put(map1);
                    } catch (Exception e) {
                        throw new JoinsException(e, "IoBuf error in sortmerge");
                    }
                    if ((map1 = p_i1.get_next()) == null) {
                        get_from_in1 = true;
                        break;
                    }
                }

                while (MapUtils.CompareMapWithMap(map2,TempMap2, jc_in1)  == 0) {
                    // Insert tuple2 into io_buf2

                    try {
                        io_buf2.Put(map2);
                    } catch (Exception e) {
                        throw new JoinsException(e, "IoBuf error in sortmerge");
                    }
                    if ((map2 = p_i2.get_next()) == null) {
                        get_from_in2 = true;
                        break;
                    }
                }

                // tuple1 and tuple2 contain the next tuples to be processed after this set.
                // Now perform a join of the tuples in io_buf1 and io_buf2.
                // This is going to be a simple nested loops join with no frills. I guess,
                // it can be made more efficient, this can be done by a future 564 student.
                // Another optimization that can be made is to choose the inner and outer
                // by checking the number of tuples in each equivalence class.

                if ((_map1 = io_buf1.Get(TempMap1)) == null)                // Should not occur
                    System.out.println("Equiv. class 1 in sort-merge has no tuples");
            }

            if ((_map2 = io_buf2.Get(TempMap2)) == null) {
                if ((_map1 = io_buf1.Get(TempMap1)) == null) {
                    process_next_block = true;
                    continue;                                // Process next equivalence class
                } else {
                    io_buf2.reread();
                    map2 = io_buf2.Get(TempMap2);
                }
            }
            if (PredEval.Eval(OutputFilter, TempMap1, TempMap2, _in1, _in2) == true) {
                Projection.Join(TempMap1, _in1,
                        TempMap2, _in2,
                        JMap, perm_mat, nOutFlds);
                return JMap;
            }
        }
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     *
     * @throws IOException    I/O error from lower layers
     * @throws JoinsException join error from lower layers
     * @throws IndexException index access error
     */
    public void close()
            throws JoinsException,
            IOException,
            IndexException {
        if (!closeFlag) {

            try {
                p_i1.close();
                p_i2.close();
            } catch (Exception e) {
                throw new JoinsException(e, "SortMerge.java: error in closing iterator.");
            }
            if (temp_file_fd1 != null) {
                try {
                    temp_file_fd1.deleteFile();
                } catch (Exception e) {
                    throw new JoinsException(e, "SortMerge.java: delete file failed");
                }
                temp_file_fd1 = null;
            }
            if (temp_file_fd2 != null) {
                try {
                    temp_file_fd2.deleteFile();
                } catch (Exception e) {
                    throw new JoinsException(e, "SortMerge.java: delete file failed");
                }
                temp_file_fd2 = null;
            }
            closeFlag = true;
        }
    }

}


