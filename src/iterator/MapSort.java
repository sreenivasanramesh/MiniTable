package iterator;

import BigT.Map;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;

import java.io.IOException;

public class MapSort extends MapIterator implements GlobalConst {

    private int[] n_Maps;
    private int n_runs;
    private static final int ARBIT_RUNS = 10;
    private short mapSize;
    private short[] str_fld_lens = null;
    private int num_cols = 4;
    private static short REC_LEN1 = 32;
    private MapIterator mapIterObj;
    private int _sort_fld;
    private TupleOrder sortOrder;
    private int _num_pages;
    private byte[][] bufs;
    private boolean first_time = true;
    private Heapfile[] temp_files;
    private int n_tempfiles;
    private Tuple output_tuple;
    private OBuf o_buf;
    private int max_elems_in_heap;
    private int sortFldLen;
    private pnodeSplayPQ queue;
    private Map op_map_buf;
    private Tuple op_tuple_buffer;
    AttrType[] mapAttributes = new AttrType[4];
    private SpoofIbuf[] i_buf;

    /**
     * Class constructor, take information about the tuples, and set up
     * the sorting
     *
     * @param attrTypes  array containing attribute types of the relation
     * @param field_sizes      array of sizes of string attributes
     * @param am             an iterator for accessing the maps
     * @param sort_fld       the field number of the field to sort on
     * @param sort_order     the sorting order (ASCENDING, DESCENDING) // it is of type tuple order but can be used for maps.
     * @param n_pages        amount of memory (attrTypes pages) available for sorting
     * @throws SortException something went wrong attrTypes the lower layer.
     */
    public MapSort(AttrType[] attrTypes, short[] field_sizes, Iterator am, int sort_fld, TupleOrder sort_order, int n_pages) throws SortException {


        int str_att_count = 0; // number of string field in maps
        for (int i = 0; i < num_cols ; i++) {
            mapAttributes[i] = new AttrType(attrTypes[i].attrType);
            if (attrTypes[i].attrType == AttrType.attrString) {
                // check if attribute is a string attribute and count them
                str_att_count++; // ideally should be 3
            }
        }

        str_fld_lens = new short[str_att_count];

        str_att_count = 0;
        for (int i = 0; i < num_cols; i++) {
            if (mapAttributes[i].attrType == AttrType.attrString) {
                str_fld_lens[str_att_count] = field_sizes[str_att_count]; // will be RECLEN1 always
                str_att_count++;
            }
        }

        //-----------------------------------------------------------------------------------------------------------//

        Map tempMap = new Map();

        try {
            tempMap.setHeader(mapAttributes, str_fld_lens); // str_fld_lens.length should always be 3
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: t.setHdr() failed");
        }
        mapSize = tempMap.size();

        mapIterObj = am; //iterator passed to the sort object.
        _sort_fld = sort_fld;
        sortOrder = sort_order;
        _num_pages = n_pages; //memory available for sorting in terms of pages

        // this may need change, bufs ???  need io_bufs.java
        //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
        PageId[] bufs_pids = new PageId[_num_pages];
        bufs = new byte[_num_pages][];

        for (int k = 0; k < _num_pages; k++) bufs[k] = new byte[MAX_SPACE];


        // as a heuristic, we set the number of runs to an arbitrary value
        // of ARBIT_RUNS
        temp_files = new Heapfile[ARBIT_RUNS];
        n_tempfiles = ARBIT_RUNS;
        n_Maps = new int[ARBIT_RUNS];
        n_runs = ARBIT_RUNS;

        try {
            temp_files[0] = new Heapfile(null);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: Heapfile error");
        }

        // just created temp heap files -----------------------------------------------------------------------------//

        o_buf = new OBuf(); //output buffer

        o_buf.init(bufs, _num_pages, mapSize, temp_files[0], false);

        max_elems_in_heap = 200;
        sortFldLen = MINIBASE_PAGESIZE;

        queue = new pnodeSplayPQ(sort_fld, attrTypes[sort_fld - 1], sortOrder);

        try {
            op_map_buf = new Map(tempMap);
            op_map_buf.setHeader(mapAttributes, str_fld_lens);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
        }
    }



    /**
     * Returns the next tuple in sorted order.
     * Note: You need to copy out the content of the tuple, otherwise it
     * will be overwritten by the next <code>get_next()</code> call.
     *
     * @return the next tuple, null if all tuples exhausted
     * @throws IOException     from lower layers
     * @throws JoinsException  from <code>generate_runs()</code>.
     * @throws LowMemException memory low exception
     * @throws Exception       other exceptions
     */
    public Map get_next() throws Exception {
        if (this.first_time) {
            // first get_next call to the sort routine
            this.first_time = false;

            // generate runs
            int nruns = generate_runs(max_elems_in_heap, mapAttributes[_sort_fld - 1], sortFldLen);

            // setup state to perform merge of runs.
            // Open input buffers for all the input file
            setup_for_merge(mapSize, nruns);
        }

        if (queue.empty()) {
            // no more maps availble
            return null;
        }

        output_tuple = delete_min();
        if (output_tuple != null) {
            op_tuple_buffer.tupleCopy(output_tuple);
            // now convert tuple op_tuple_buffer to map
            return op_map_buf;
        } else
            return null;
    }



    /**
     * Generate sorted runs.
     * Using heap sort.
     *
     * @param max_elems   maximum number of elements in heap
     * @param sortFldType attribute type of the sort field
     * @param sortFldLen  length of the sort field
     * @return number of runs generated
     * @throws IOException    from lower layers
     * @throws SortException  something went wrong in the lower layer.
     * @throws JoinsException from <code>Iterator.get_next()</code>
     */
    private int generate_runs(int max_elems, AttrType sortFldType, int sortFldLen) throws Exception {
        Tuple tuple;
        Map map;
        pnode cur_node;
        pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, sortFldType, sortOrder);
        pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, sortFldType, sortOrder);
        pnodeSplayPQ pcurr_Q = Q1;
        pnodeSplayPQ pother_Q = Q2;
        //Tuple lastElem = new Tuple(mapSize);  // need tuple.java
        Map lastElem = new Map();
        try {
            lastElem.setHeader(mapAttributes, str_fld_lens);
            //lastElem.setHdr((short) num_cols, mapAttributes, str_fld_lens);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: setHdr() failed");
        }

        int run_num = 0;  // keeps track of the number of runs

        // number of elements in Q
        //    int nelems_Q1 = 0;
        //    int nelems_Q2 = 0;
        int p_elems_curr_Q = 0;
        int p_elems_other_Q = 0;

        int comp_res;

        // set the lastElem to be the minimum value for the sort field
        if (sortOrder.tupleOrder == TupleOrder.Ascending) {
            try {
                MIN_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MIN_VAL failed");
            }
        } else {
            try {
                MAX_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MIN_VAL failed");
            }
        }

        // maintain a fixed maximum number of elements in the heap
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
            try {
                map = mapIterObj.get_next();  // according to Iterator.java should return a tuple or convert map to tuple
            } catch (Exception e) {
                e.printStackTrace();
                throw new SortException(e, "Sort.java: get_next() failed");
            }

            if (map == null) {
                break;
            }
            cur_node = new pnode();
            //cur_node.tuple = new Tuple(tuple); // tuple copy needed --  Bingjie 4/29/98
            cur_node.map = new Map(map);
            pcurr_Q.enq(cur_node);
            p_elems_curr_Q++;
        }

        // now the queue is full, starting writing to file while keep trying
        // to add new tuples to the queue. The ones that does not fit are put
        // on the other queue temperarily
        while (true) {
            cur_node = pcurr_Q.deq();
            if (cur_node == null) break;
            p_elems_curr_Q--;

            //comp_res = TupleUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, _sort_fld, lastElem);  // need tuple_utils.java
            comp_res = MapUtils.CompareMapWithValue(cur_node.map, _sort_fld, lastElem);

            if ((comp_res < 0 && sortOrder.tupleOrder == TupleOrder.Ascending) || (comp_res > 0 && sortOrder.tupleOrder == TupleOrder.Descending)) {
                // doesn't fit in current run, put into the other queue
                try {
                    pother_Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                }
                p_elems_other_Q++;
            } else {
                // set lastElem to have the value of the current tuple,
                // need tuple_utils.java
                //TupleUtils.SetValue(lastElem, cur_node.tuple, _sort_fld, sortFldType);
                MapUtils.SetValue(lastElem, cur_node.map, _sort_fld, sortFldType);
                // write tuple to output file, need io_bufs.java, type cast???
                //	System.out.println("Putting tuple into run " + (run_num + 1));
                //	cur_node.tuple.print(_in);

                //o_buf.Put(cur_node.tuple);
                o_buf.Put(cur_node.map);
            }

            // check whether the other queue is full
            if (p_elems_other_Q == max_elems) {
                // close current run and start next run
                n_Maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
                run_num++;

                // check to see whether need to expand the array
                if (run_num == n_tempfiles) {
                    Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
                    if (n_tempfiles >= 0) System.arraycopy(temp_files, 0, temp1, 0, n_tempfiles);
                    temp_files = temp1;
                    n_tempfiles *= 2;

                    int[] temp2 = new int[2 * n_runs];
                    if (n_runs >= 0) System.arraycopy(n_Maps, 0, temp2, 0, n_runs);
                    n_Maps = temp2;
                    n_runs *= 2;
                }

                try {
                    temp_files[run_num] = new Heapfile(null);
                } catch (Exception e) {
                    throw new SortException(e, "Sort.java: create Heapfile failed");
                }

                // need io_bufs.java
                o_buf.init(bufs, _num_pages, mapSize, temp_files[run_num], false);

                // set the last Elem to be the minimum value for the sort field
                if (sortOrder.tupleOrder == TupleOrder.Ascending) {
                    try {
                        MIN_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                } else {
                    try {
                        MAX_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                }

                // switch the current heap and the other heap
                pnodeSplayPQ tempQ = pcurr_Q;
                pcurr_Q = pother_Q;
                pother_Q = tempQ;
                int tempelems = p_elems_curr_Q;
                p_elems_curr_Q = p_elems_other_Q;
                p_elems_other_Q = tempelems;
            }

            // now check whether the current queue is empty
            else if (p_elems_curr_Q == 0) {
                while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
                    try {
                        map = mapIterObj.get_next();  // according to Iterator.java should return a tuple or convert map to tuple.
                    } catch (Exception e) {
                        throw new SortException(e, "get_next() failed");
                    }

                    if (map == null) {
                        break;
                    }
                    cur_node = new pnode();
                    //cur_node.tuple = new Tuple(tuple); // tuple copy needed --  Bingjie 4/29/98
                    cur_node.map = new Map();
                    try {
                        pcurr_Q.enq(cur_node);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                    }
                    p_elems_curr_Q++;
                }
            }

            // Check if we are done
            if (p_elems_curr_Q == 0) {
                // current queue empty despite our attemps to fill in
                // indicating no more tuples from input
                if (p_elems_other_Q == 0) {
                    // other queue is also empty, no more tuples to write out, done
                    break; // of the while(true) loop
                } else {
                    // generate one more run for all tuples in the other queue
                    // close current run and start next run
                    n_Maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
                    run_num++;

                    // check to see whether need to expand the array
                    if (run_num == n_tempfiles) {
                        Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
                        if (n_tempfiles >= 0) System.arraycopy(temp_files, 0, temp1, 0, n_tempfiles);
                        temp_files = temp1;
                        n_tempfiles *= 2;

                        int[] temp2 = new int[2 * n_runs];
                        if (n_runs >= 0) System.arraycopy(n_Maps, 0, temp2, 0, n_runs);
                        n_Maps = temp2;
                        n_runs *= 2;
                    }

                    try {
                        temp_files[run_num] = new Heapfile(null);
                    } catch (Exception e) {
                        throw new SortException(e, "Sort.java: create Heapfile failed");
                    }

                    // need io_bufs.java
                    o_buf.init(bufs, _num_pages, mapSize, temp_files[run_num], false);

                    // set the last Elem to be the minimum value for the sort field
                    if (sortOrder.tupleOrder == TupleOrder.Ascending) {
                        try {
                            MIN_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    } else {
                        try {
                            MAX_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    }

                    // switch the current heap and the other heap
                    pnodeSplayPQ tempQ = pcurr_Q;
                    pcurr_Q = pother_Q;
                    pother_Q = tempQ;
                    int tempelems = p_elems_curr_Q;
                    p_elems_curr_Q = p_elems_other_Q;
                    p_elems_other_Q = tempelems;
                }
            } // end of if (p_elems_curr_Q == 0)
        } // end of while (true)

        // close the last run
        n_Maps[run_num] = (int) o_buf.flush();
        run_num++;

        return run_num;
    }

    /**
     * Set up for merging the runs.
     * Open an input buffer for each run, and insert the first element (min)
     * from each run into a heap. <code>delete_min() </code> will then get
     * the minimum of all runs.
     *
     * @param tuple_size size (in bytes) of each tuple
     * @param n_R_runs   number of runs
     * @throws IOException     from lower layers
     * @throws LowMemException there is not enough memory to
     *                         sort in two passes (a subclass of SortException).
     * @throws SortException   something went wrong in the lower layer.
     * @throws Exception       other exceptions
     */
    // TODO: modify tuple to map.
    private void setup_for_merge(int tuple_size, int n_R_runs)
            throws IOException,
            LowMemException,
            SortException,
            Exception {
        // don't know what will happen if n_R_runs > _n_pages
        if (n_R_runs > _num_pages)
            throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");

        int i;
        pnode cur_node;  // need pq_defs.java

        i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
        for (int j = 0; j < n_R_runs; j++) i_buf[j] = new SpoofIbuf();

        // construct the lists, ignore TEST for now
        // this is a patch, I am not sure whether it works well -- bingjie 4/20/98

        for (i = 0; i < n_R_runs; i++) {
            byte[][] apage = new byte[1][];
            apage[0] = bufs[i];

            // need iobufs.java
            i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_Maps[i]);

            cur_node = new pnode();
            cur_node.run_num = i;

            // may need change depending on whether Get() returns the original
            // or make a copy of the tuple, need io_bufs.java ???
            Tuple temp_tuple = new Tuple(tuple_size);

            try {
                temp_tuple.setHdr((short) num_cols, mapAttributes, str_fld_lens);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
            }

            temp_tuple = i_buf[i].Get(temp_tuple);  // need io_bufs.java

            if (temp_tuple != null) {
	/*
	System.out.print("Get tuple from run " + i);
	temp_tuple.print(_in);
	*/
                cur_node.tuple = temp_tuple; // no copy needed
                try {
                    queue.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (TupleUtilsException e) {
                    throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
                }

            }
        }
    }

    /**
     * Remove the minimum value among all the runs.
     *
     * @return the minimum tuple removed
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    private Map delete_min() throws Exception {
        pnode cur_node;                // needs pq_defs.java
        //Tuple new_tuple, old_tuple;
        Map newMap, oldMap;

        cur_node = queue.deq();
        //old_tuple = cur_node.tuple;
        oldMap = cur_node.map;

    /*
    System.out.print("Get ");
    old_tuple.print(_in);
    */
        // we just removed one tuple from one run, now we need to put another
        // tuple of the same run into the queue
        if (!i_buf[cur_node.run_num].empty()) {
            // run not exhausted
            try {
                //new_tuple = new Tuple(mapSize);
                newMap = new Map();
                newMap.setHeader( mapAttributes, str_fld_lens);
                //new_tuple.setHdr((short) num_cols, mapAttributes, str_fld_lens);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: setHdr() failed");
            }

            //new_tuple = i_buf[cur_node.run_num].Get(new_tuple);
            newMap = i_buf[cur_node.run_num].Get(newMap);
            if (newMap != null) {
	/*
	System.out.print(" fill in from run " + cur_node.run_num);
	new_tuple.print(_in);
	*/
                //cur_node.tuple = new_tuple;  // no copy needed -- I think Bingjie 4/22/98
                cur_node.map = newMap;
                try {
                    queue.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (TupleUtilsException e) {
                    throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
                }
            } else {
                throw new SortException("********** Wait a minute, I thought input is not empty ***************");
            }

        }

        // changed to return Tuple instead of return char array ????
        //return old_tuple;
        return oldMap;
    }

    /**
     * Set lastElem to be the minimum value of the appropriate type
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MIN_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);
        char[] c = new char[1];
        String s = new String(c);
        //    short fld_no = 1;

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld-no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
                break;
            case AttrType.attrString:
                //      lastElem.setHdr(fld_no, junk, s_size);
                lastElem.setStrFld(_sort_fld, s);
                break;
            default:
                // don't know how to handle attrSymbol, attrNull
                //System.err.println("error in sort.java");
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

    }

    /**
     * Set lastElem to be the maximum value of the appropriate type
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MAX_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);
        char[] c = new char[1];
        c[0] = Character.MAX_VALUE;
        String s = new String(c);
        //    short fld_no = 1;

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
                break;
            case AttrType.attrString:
                //      lastElem.setHdr(fld_no, junk, s_size);
                lastElem.setStrFld(_sort_fld, s);
                break;
            default:
                // don't know how to handle attrSymbol, attrNull
                //System.err.println("error in sort.java");
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

    }

    @Override
    public void close() throws SortException {
        // clean up
        if (!closeFlag) {

            try {
                mapIterObj.close();
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: error in closing iterator.");
            }


            for (int i = 0; i < temp_files.length; i++) {
                if (temp_files[i] != null) {
                    try {
                        temp_files[i].deleteFile();
                    } catch (Exception e) {
                        throw new SortException(e, "Sort.java: Heapfile error");
                    }
                    temp_files[i] = null;
                }
            }
            closeFlag = true;
        }
    }
}