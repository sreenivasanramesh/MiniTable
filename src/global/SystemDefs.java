package global;

import bufmgr.BufMgr;
import catalog.Catalog;
import diskmgr.bigDB;

import java.io.File;

public class SystemDefs {
    public static BufMgr JavabaseBM;
    public static bigDB JavabaseDB;
    public static Catalog JavabaseCatalog;

    public static String JavabaseDBName;
    public static String JavabaseLogName;
    public static boolean MINIBASE_RESTART_FLAG = false;
    public static String MINIBASE_DBNAME;

    public SystemDefs() {
    }

    public SystemDefs(String dbname, int num_pgs, int bufpoolsize,
                      String replacement_policy) {
        int logsize;

        String real_logname = dbname;
        String real_dbname = dbname;

        if (num_pgs == 0) {
            logsize = 500;
        } else {
            logsize = 3 * num_pgs;
        }

        if (replacement_policy == null) {
            replacement_policy = "Clock";
        }

        init(real_dbname, real_logname, num_pgs, logsize,
                bufpoolsize, replacement_policy);
    }


    public void init(String dbname, String logname,
                     int num_pgs, int maxlogsize,
                     int bufpoolsize, String replacement_policy) {

        boolean status = true;
        JavabaseBM = null;
        JavabaseDB = null;
        JavabaseDBName = null;
        JavabaseLogName = null;
        JavabaseCatalog = null;

        try {
            JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
            JavabaseDB = new bigDB();
/*
	JavabaseCatalog = new Catalog(); 
*/
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        JavabaseDBName = dbname;
        JavabaseLogName = logname;
        MINIBASE_DBNAME = JavabaseDBName;

//        // create or open the DB
    
        File file = new File(dbname);
        if(file.exists()){
            MINIBASE_RESTART_FLAG = true;
        }
        
        if ((MINIBASE_RESTART_FLAG) || (num_pgs == 0)) {//open an existing database
            try {
                JavabaseDB.openDB(dbname);
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        } else {
            try {
                JavabaseDB.openDB(dbname, num_pgs);
                JavabaseBM.flushAllPages();
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
    }
}
