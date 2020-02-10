package global;

public interface GlobalConst {

    public static final int MINIBASE_MAXARRSIZE = 50;
    public static final int NUMBUF = 50;

    /**
     * Size of page.
     */
    public static final int MINIBASE_PAGESIZE = 1024;           // in bytes

    /**
     * Size of each frame.
     */
    public static final int MINIBASE_BUFFER_POOL_SIZE = 1024;   // in Frames

    public static final int MAX_SPACE = 1024;   // in Frames

    /**
     * in Pages => the DBMS Manager tells the DB how much disk
     * space is available for the database.
     */
    public static final int MINIBASE_DB_SIZE = 10000;
    public static final int MINIBASE_MAX_TRANSACTIONS = 100;
    public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;

    /**
     * also the name of a relation
     */
    public static final int MAXFILENAME = 15;
    public static final int MAXINDEXNAME = 40;
    public static final int MAXATTRNAME = 15;
    public static final int MAX_NAME = 50;

    public static final int INVALID_PAGE = -1;
}
