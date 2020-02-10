package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;

import java.io.*;


/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class IndexScan extends Iterator {

    /**
     * class constructor. set up the index scan.
     *
     * @param index     type of the index (B_Index, Hash)
     * @param relName   name of the input relation
     * @param indName   name of the input index
     * @param types     array of types in this relation
     * @param str_sizes array of string sizes (for attributes that are string)
     * @param noInFlds  number of fields in input tuple
     * @param noOutFlds number of fields in output tuple
     * @param outFlds   fields to project
     * @param selects   conditions to apply, first one is primary
     * @param fldNum    field number of the indexed field
     * @param indexOnly whether the answer requires only the key or the tuple
     * @throws IndexException            error from the lower layer
     * @throws InvalidTypeException      tuple type not valid
     * @throws InvalidTupleSizeException tuple size not valid
     * @throws UnknownIndexTypeException index type unknown
     * @throws IOException               from the lower layer
     */
    public IndexScan(
            IndexType index,
            final String relName,
            final String indName,
            AttrType types[],
            short str_sizes[],
            int noInFlds,
            int noOutFlds,
            FldSpec outFlds[],
            CondExpr selects[],
            final int fldNum,
            final boolean indexOnly
    )
            throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            IOException {
        _fldNum = fldNum;
        _noInFlds = noInFlds;
        _types = types;
        _s_sizes = str_sizes;

        AttrType[] Jtypes = new AttrType[noOutFlds];
        short[] ts_sizes;
        Jtuple = new Tuple();

        try {
            ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
        } catch (TupleUtilsException e) {
            throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
        } catch (InvalidRelation e) {
            throw new IndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
        }

        _selects = selects;
        perm_mat = outFlds;
        _noOutFlds = noOutFlds;
        tuple1 = new Tuple();
        try {
            tuple1.setHdr((short) noInFlds, types, str_sizes);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        t1_size = tuple1.size();
        index_only = indexOnly;  // added by bingjie miao

        try {
            f = new Heapfile(relName);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch (index.indexType) {
            // linear hashing is not yet implemented
            case IndexType.B_Index:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indFile = new BTreeFile(indName);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }

    }

    /**
     * returns the next tuple.
     * if <code>index_only</code>, only returns the key value
     * (as the first field in a tuple)
     * otherwise, retrive the tuple and returns the whole tuple
     *
     * @return the tuple
     * @throws IndexException          error from the lower layer
     * @throws UnknownKeyTypeException key type unknown
     * @throws IOException             from the lower layer
     */
    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException {
        RID rid;
        int unused;
        KeyDataEntry nextentry = null;

        try {
            nextentry = indScan.get_next();
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTree error");
        }

        while (nextentry != null) {
            if (index_only) {
                // only need to return the key

                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[1];

                if (_types[_fldNum - 1].attrType == AttrType.attrInteger) {
                    attrType[0] = new AttrType(AttrType.attrInteger);
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
                    } catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }
                } else if (_types[_fldNum - 1].attrType == AttrType.attrString) {

                    attrType[0] = new AttrType(AttrType.attrString);
                    // calculate string size of _fldNum
                    int count = 0;
                    for (int i = 0; i < _fldNum; i++) {
                        if (_types[i].attrType == AttrType.attrString)
                            count++;
                    }
                    s_sizes[0] = _s_sizes[count - 1];

                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
                    } catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }
                } else {
                    // attrReal not supported for now
                    throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                }
                return Jtuple;
            }

            // not index_only, need to return the whole tuple
            rid = ((LeafData) nextentry.data).getData();
            try {
                tuple1 = f.getRecord(rid);
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: getRecord failed");
            }

            try {
                tuple1.setHdr((short) _noInFlds, _types, _s_sizes);
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: Heapfile error");
            }

            boolean eval;
            try {
                eval = PredEval.Eval(_selects, tuple1, null, _types, null);
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: Heapfile error");
            }

            if (eval) {
                // need projection.java
                try {
                    Projection.Project(tuple1, _types, Jtuple, perm_mat, _noOutFlds);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: Heapfile error");
                }

                return Jtuple;
            }

            try {
                nextentry = indScan.get_next();
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: BTree error");
            }
        }

        return null;
    }

    /**
     * Cleaning up the index scan, does not remove either the original
     * relation or the index from the database.
     *
     * @throws IndexException error from the lower layer
     * @throws IOException    from the lower layer
     */
    public void close() throws IOException, IndexException {
        if (!closeFlag) {
            if (indScan instanceof BTFileScan) {
                try {
                    ((BTFileScan) indScan).DestroyBTreeFileScan();
                } catch (Exception e) {
                    throw new IndexException(e, "BTree error in destroying index scan.");
                }
            }

            closeFlag = true;
        }
    }

    public FldSpec[] perm_mat;
    private IndexFile indFile;
    private IndexFileScan indScan;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private int _noInFlds;
    private int _noOutFlds;
    private Heapfile f;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int t1_size;
    private int _fldNum;
    private boolean index_only;

}

