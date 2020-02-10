package btree;

import java.io.*;

import global.*;


/**
 * Contains the enumerated types of state of the scan
 */
class ScanState {
    public static final int NEWSCAN = 0;
    public static final int SCANRUNNING = 1;
    public static final int SCANCOMPLETE = 2;
}

/**
 * Base class for a index file
 */
public abstract class IndexFile {
    /**
     * Insert entry into the index file.
     *
     * @param data the key for the entry
     * @param rid  the rid of the tuple with the key
     * @throws IOException             from lower layers
     * @throws KeyTooLongException     the key is too long
     * @throws KeyNotMatchException    the keys do not match
     * @throws LeafInsertRecException  insert record to leaf page failed
     * @throws IndexInsertRecException insert record to index page failed
     * @throws ConstructPageException  fail to construct a header page
     * @throws UnpinPageException      unpin page failed
     * @throws PinPageException        pin page failed
     * @throws NodeNotMatchException   nodes do not match
     * @throws ConvertException        conversion failed (from global package)
     * @throws DeleteRecException      delete record failed
     * @throws IndexSearchException    index search failed
     * @throws IteratorException       error from iterator
     * @throws LeafDeleteException     delete leaf page failed
     * @throws InsertException         insert record failed
     */
    abstract public void insert(final KeyClass data, final RID rid)
            throws KeyTooLongException,
            KeyNotMatchException,
            LeafInsertRecException,
            IndexInsertRecException,
            ConstructPageException,
            UnpinPageException,
            PinPageException,
            NodeNotMatchException,
            ConvertException,
            DeleteRecException,
            IndexSearchException,
            IteratorException,
            LeafDeleteException,
            InsertException,
            IOException;

    /**
     * Delete entry from the index file.
     *
     * @param data the key for the entry
     * @param rid  the rid of the tuple with the key
     * @throws IOException               from lower layers
     * @throws DeleteFashionException    delete fashion undefined
     * @throws LeafRedistributeException failed to redistribute leaf page
     * @throws RedistributeException     redistrubtion failed
     * @throws InsertRecException        insert record failed
     * @throws KeyNotMatchException      keys do not match
     * @throws UnpinPageException        unpin page failed
     * @throws IndexInsertRecException   insert record to index failed
     * @throws FreePageException         free page failed
     * @throws RecordNotFoundException   failed to find the record
     * @throws PinPageException          pin page failed
     * @throws IndexFullDeleteException  full delete on index page failed
     * @throws LeafDeleteException       delete leaf page failed
     * @throws IteratorException         exception from iterating through records
     * @throws ConstructPageException    fail to construct the header page
     * @throws DeleteRecException        delete record failed
     * @throws IndexSearchException      index search failed
     */
    abstract public boolean Delete(final KeyClass data, final RID rid)
            throws DeleteFashionException,
            LeafRedistributeException,
            RedistributeException,
            InsertRecException,
            KeyNotMatchException,
            UnpinPageException,
            IndexInsertRecException,
            FreePageException,
            RecordNotFoundException,
            PinPageException,
            IndexFullDeleteException,
            LeafDeleteException,
            IteratorException,
            ConstructPageException,
            DeleteRecException,
            IndexSearchException,
            IOException;
}
