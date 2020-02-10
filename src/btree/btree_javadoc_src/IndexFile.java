package btree;

import java.io.*;

import global.*;

/**
 * Base class for a index file
 */
public abstract class IndexFile {
    /**
     * Insert entry into the index file.
     *
     * @param data the key for the entry
     * @param rid  the rid of the tuple with the key
     */
    abstract public void insert(final KeyClass data, final RID rid);

    /**
     * Delete entry from the index file.
     *
     * @param data the key for the entry
     * @param rid  the rid of the tuple with the key
     */
    abstract public boolean Delete(final KeyClass data, final RID rid);
}
