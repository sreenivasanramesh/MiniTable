/////////////////////////////////////////////////////////////////
//
// filename : ext_sys_defs.java		Ning Wang, April 25, 1998
//
// This extends the basic system globals to include the catalog
// objects.
//
//
/////////////////////////////////////////////////////////////////

package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;

public class ExtendedSystemDefs extends SystemDefs {
  /* This class actually allocates the global catalog pointer.  Use this class
     rather than SystemDefs when you need to use the catalog system. */

    public ExtendedSystemDefs(String dbname, int dbpages,
                              int bufpoolsize,
                              String replacement_policy) {
        super(dbname, dbpages, bufpoolsize, replacement_policy);
        init(dbpages);
    }

    public ExtendedSystemDefs(String dbname, String logname,
                              int dbpages, int maxlogsize,
                              int bufpoolsize,
                              String replacement_policy) {
        super(dbname, dbpages, bufpoolsize, replacement_policy);
        init(dbpages);
    }

    public void init(int initCatalog) {

        JavabaseCatalog = new Catalog();


        MINIBASE_CATALOGPTR = SystemDefs.JavabaseCatalog;
        MINIBASE_ATTRCAT = MINIBASE_CATALOGPTR.getAttrCat();
        MINIBASE_RELCAT = MINIBASE_CATALOGPTR.getRelCat();
        MINIBASE_INDCAT = MINIBASE_CATALOGPTR.getIndCat();
    }


    public static Catalog
            MINIBASE_CATALOGPTR;

    public static AttrCatalog
            MINIBASE_ATTRCAT;

    public static RelCatalog
            MINIBASE_RELCAT;

    public static IndexCatalog
            MINIBASE_INDCAT;

}
