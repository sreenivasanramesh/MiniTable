package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;

public interface Catalogglobal {

    final static int MINIBASE_MAXARRSIZE = 50;

    // Global constants defined in CATALOG

    final static String RELCATNAME = new String("relcat");  // name of relation catalog
    final static String ATTRCATNAME = new String("attrcat"); // name of attribute catalog
    final static String INDEXCATNAME = new String("indcat");
    final static String RELNAME = new String("relname"); // name of indexed field in rel/attrcat
    final static int MAXNAME = 32; // length of relName, attrName
    final static int MAXSTRINGLEN = 255; // max. length of string attribute
    final static int NUMTUPLESFILE = 100; // default statistic = no recs in file
    final static int NUMPAGESFILE = 20; // default statistic = no pages in file
    final static int DISTINCTKEYS = 20; // default statistic: no of distinct keys
    final static int INDEXPAGES = 5; // default statisitc no of index pages
    final static String MINSTRINGVAL = new String("A"); // default statisitic
    final static String MAXSTRINGVAL = new String("ZZZZZZZ"); // default statisitic
    final static int MINNUMVAL = 0;
    final static int MAXNUMVAL = 999999999;

}

