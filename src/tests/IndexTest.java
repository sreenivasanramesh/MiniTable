package tests;

import java.io.*; 
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import btree.*; 
import java.util.Random;


class IndexDriver extends TestDriver 
  implements GlobalConst {

  private static String   data1[] = {
	"raghu", "xbao", "cychan", "leela", "ketola", "soma", "ulloa", 
	"dhanoa", "dsilva", "kurniawa", "dissoswa", "waic", "susanc", "kinc", 
	"marc", "scottc", "yuc", "ireland", "rathgebe", "joyce", "daode", 
	"yuvadee", "he", "huxtable", "muerle", "flechtne", "thiodore", "jhowe",
	"frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch",
	"binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
	"chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel", 
	"newell", "schiesl", "neuman", "heitzman", "wan", "gunawan", "djensen",
	"juei-wen", "josephin", "harimin", "xin", "zmudzin", "feldmann", 
	"joon", "wawrzon", "yi-chun", "wenchao", "seo", "karsono", "dwiyono", 
	"ginther", "keeler", "peter", "lukas", "edwards", "mirwais","schleis", 
	"haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
	"honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey", 
	"awny", "savoy", "meltz"}; 
      
  private static String   data2[] = {
	"andyw", "awny", "azat", "barthel", "binh", "bloch", "bradw", 
	"chingju", "chui", "chung-pi", "cychan", "dai", "daode", "dhanoa", 
	"dissoswa", "djensen", "dsilva", "dwiyono", "edwards", "evgueni", 
	"feldmann", "flechtne", "frankief", "ginther", "gray", "guangshu", 
	"gunawan", "hai", "handi", "harimin", "haris", "he", "heitzman", 
	"honghu", "huxtable", "ireland", "jhowe", "joon", "josephin", "joyce",
	"jsong", "juei-wen", "karsono", "keeler", "ketola", "kinc", "kurniawa",
	"leela", "lukas", "mak", "marc", "markert", "meltz", "meyers", 
	"mirwais", "muerle", "muthiah", "neuman", "newell", "peter", "raghu", 
	"randal", "rathgebe", "robert", "savoy", "schiesl", "schleis", 
	"scottc", "seo", "shi", "shun-kit", "siddiqui", "soma", "sonthi", 
	"sungk", "susanc", "tak", "thiodore", "ulloa", "vharvey", "waic",
	"wan", "wawrzon", "wenchao", "wlau", "xbao", "xiaoming", "xin", 
	"yi-chun", "yiching", "yuc", "yung", "yuvadee", "zmudzin" };

  private static int   NUM_RECORDS = data2.length; 
  private static int   LARGE = 1000; 
  private static short REC_LEN1 = 32; 
  private static short REC_LEN2 = 160; 


  public IndexDriver() {
    super("indextest");
  }

  public boolean runTests ()  {
    
    System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
    
    SystemDefs sysdef = new SystemDefs( dbpath, 300, NUMBUF, "Clock" );

    // Kill anything that might be hanging around
    String newdbpath;
    String newlogpath;
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";

    newdbpath = dbpath;
    newlogpath = logpath;

    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;

    // Commands here is very machine dependent.  We assume
    // user are on UNIX system here
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } 
    catch (IOException e) {
      System.err.println (""+e);
    }
    
    remove_logcmd = remove_cmd + newlogpath;
    remove_dbcmd = remove_cmd + newdbpath;

    //This step seems redundant for me.  But it's in the original
    //C++ code.  So I am keeping it as of now, just in case I
    //I missed something
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } 
    catch (IOException e) {
      System.err.println (""+e);
    }

    //Run the tests. Return type different from C++
    boolean _pass = runAllTests();

    //Clean up again
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } 
    catch (IOException e) {
      System.err.println (""+e);
    }
    
    System.out.println ("\n" + "..." + testName() + " tests ");
    System.out.println (_pass==OK ? "completely successfully" : "failed");
    System.out.println (".\n\n");
    
    return _pass;
  }

  protected boolean test1()
  {
    System.out.println("------------------------ TEST 1 --------------------------");
    
    boolean status = OK;

    AttrType[] attrType = new AttrType[2];
    attrType[0] = new AttrType(AttrType.attrString);
    attrType[1] = new AttrType(AttrType.attrString);
    short[] attrSize = new short[2];
    attrSize[0] = REC_LEN2;
    attrSize[1] = REC_LEN1;
    
    // create a tuple of appropriate size
    Tuple t = new Tuple();
    try {
      t.setHdr((short) 2, attrType, attrSize);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int size = t.size();
    
    // Create unsorted data file "test1.in"
    RID             rid;
    Heapfile        f = null;
    try {
      f = new Heapfile("test1.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 2, attrType, attrSize);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<NUM_RECORDS; i++) {
      try {
	t.setStrFld(2, data1[i]);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }

    // create an scan on the heapfile
    Scan scan = null;
    
    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrString, REC_LEN1, 1/*delete*/); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    System.out.println("BTreeIndex created successfully.\n"); 
    
    rid = new RID();
    String key = null;
    Tuple temp = null;
    
    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      t.tupleCopy(temp);
      
      try {
	key = t.getStrFld(2);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btf.insert(new StringKey(key), rid); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scan.closescan();
    
    System.out.println("BTreeIndex file created successfully.\n"); 
    
    FldSpec[] projlist = new FldSpec[2];
    RelSpec rel = new RelSpec(RelSpec.outer); 
    projlist[0] = new FldSpec(rel, 1);
    projlist[1] = new FldSpec(rel, 2);
    
    // start index scan
    IndexScan iscan = null;
    try {
      iscan = new IndexScan(new IndexType(IndexType.B_Index), "test1.in", "BTreeIndex", attrType, attrSize, 2, 2, projlist, null, 2, true);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    

    int count = 0;
    t = null;
    String outval = null;
    
    try {
      t = iscan.get_next();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace(); 
    }

    boolean flag = true;
    
    while (t != null) {
      if (count >= NUM_RECORDS) {
	System.err.println("Test1 -- OOPS! too many records");
	status = FAIL;
	flag = false; 
	break;
      }
      
      try {
	outval = t.getStrFld(1);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      if (outval.compareTo(data2[count]) != 0) {
	System.err.println("outval = " + outval + "\tdata2[count] = " + data2[count]);
	
	System.err.println("Test1 -- OOPS! index scan not in sorted order");
	status = FAIL;
      }
      count++;

      try {
	t = iscan.get_next();
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    if (count < NUM_RECORDS) {
	System.err.println("Test1 -- OOPS! too few records");
	status = FAIL;
    }
    else if (flag && status) {
      System.err.println("Test1 -- Index Scan OK");
    }

    // clean up
    try {
      iscan.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    System.err.println("------------------- TEST 1 completed ---------------------\n");
    
    return status;
  }


  protected boolean test2()
  {
    System.out.println("------------------------ TEST 2 --------------------------");
    
    boolean status = OK;

    AttrType[] attrType = new AttrType[2];
    attrType[0] = new AttrType(AttrType.attrString);
    attrType[1] = new AttrType(AttrType.attrString);
    short[] attrSize = new short[2];
    attrSize[0] = REC_LEN2;
    attrSize[1] = REC_LEN1;
    
    // create a tuple of appropriate size
    Tuple t = new Tuple();
    try {
      t.setHdr((short) 2, attrType, attrSize);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int size = t.size();
    
    RID             rid;
    Heapfile        f = null;

    // open existing data file
    try {
      f = new Heapfile("test1.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 2, attrType, attrSize);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    // open existing index
    BTreeFile btf =  null;
    try {
      btf = new BTreeFile("BTreeIndex");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    System.out.println("BTreeIndex opened successfully.\n"); 
    
    rid = new RID();
    String key = null;
    Tuple temp = null;
    

    FldSpec[] projlist = new FldSpec[2];
    RelSpec rel = new RelSpec(RelSpec.outer); 
    projlist[0] = new FldSpec(rel, 1);
    projlist[1] = new FldSpec(rel, 2);
    
    // set up an identity selection
    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrString);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr[0].operand2.string = "dsilva";
    expr[0].next = null;
    expr[1] = null;
    
    // start index scan
    IndexScan iscan = null;
    try {
      iscan = new IndexScan(new IndexType(IndexType.B_Index), "test1.in", "BTreeIndex", attrType, attrSize, 2, 2, projlist, expr, 2, false);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    

    int count = 0;
    t = null;
    String outval = null;
    
    try {
      t = iscan.get_next();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace(); 
    }

    if (t == null) {
      System.err.println("Test 2 -- no record retrieved from identity search.");
      status = FAIL;
      return status; 
    }

    try {
      outval = t.getStrFld(2);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (outval.compareTo("dsilva") != 0) {
      System.err.println("Test2 -- error in identity search.");
      status = FAIL;
    }
    
    try {
      t = iscan.get_next();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace(); 
    }
    
    if (t != null) {
      System.err.println("Test2 -- OOPS! too many records");
      status = FAIL;
    }
    
    // clean up
    try {
      iscan.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    // now try a range scan
    expr = new CondExpr[3]; 
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopGE);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrString);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr[0].operand2.string = "dsilva";
    expr[0].next = null;
    expr[1] = new CondExpr();
    expr[1].op = new AttrOperator(AttrOperator.aopLE);
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrString);
    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr[1].operand2.string = "yuc";
    expr[1].next = null;
    expr[2] = null;
    
    // start index scan
    iscan = null;
    try {
      iscan = new IndexScan(new IndexType(IndexType.B_Index), "test1.in", "BTreeIndex", attrType, attrSize, 2, 2, projlist, expr, 2, false);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    

    count = 16; // because starting from dsilva
    t = null;
    
    try {
      t = iscan.get_next();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace(); 
    }

    boolean flag = true;
 
    while (t != null) {
      if (count >= (NUM_RECORDS - 3)) {
	System.err.println("Test2 -- OOPS! too many records");
	status = FAIL;
	flag = false; 
	break;
      }
      
      try {
	outval = t.getStrFld(2);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      if (outval.compareTo(data2[count]) != 0) {
	System.err.println("outval = " + outval + "\tdata2[count] = " + data2[count]);
	
	System.err.println("Test2 -- OOPS! index scan not in sorted order");
	status = FAIL;
      }
      count++;

      try {
	t = iscan.get_next();
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    if (count < (NUM_RECORDS-3)) {
	System.err.println("Test2 -- OOPS! too few records");
	status = FAIL;
    }
    else if (flag && status) {
      System.err.println("Test2 -- Index Scan OK");
    }
    
    // clean up
    try {
      iscan.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    System.err.println("------------------- TEST 2 completed ---------------------\n");
        
    return status;
  }


  protected boolean test3()
  { 
    System.out.println("------------------------ TEST 3 --------------------------");
    
    boolean status = OK;

    Random random1 = new Random();
    Random random2 = new Random();
    
    AttrType[] attrType = new AttrType[4];
    attrType[0] = new AttrType(AttrType.attrString);
    attrType[1] = new AttrType(AttrType.attrString);
    attrType[2] = new AttrType(AttrType.attrInteger);
    attrType[3] = new AttrType(AttrType.attrReal);
    short[] attrSize = new short[2];
    attrSize[0] = REC_LEN1;
    attrSize[1] = REC_LEN1;
    
    Tuple t = new Tuple();

    try {
      t.setHdr((short) 4, attrType, attrSize);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    int size = t.size();

    // Create unsorted data file "test3.in"
    RID             rid;
    Heapfile        f = null;
    try {
      f = new Heapfile("test3.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 4, attrType, attrSize);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int inum = 0;
    float fnum = 0;
    int count = 0;
    
    for (int i=0; i<LARGE; i++) {
      // setting fields
      inum = random1.nextInt();
      fnum = random2.nextFloat();
      try {
	t.setStrFld(1, data1[i%NUM_RECORDS]);
	t.setIntFld(3, inum%1000);
	t.setFloFld(4, fnum);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }

    // create an scan on the heapfile
    Scan scan = null;
    
    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file on the integer field
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTIndex", AttrType.attrInteger, 4, 1/*delete*/); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    System.out.println("BTreeIndex created successfully.\n"); 
    
    rid = new RID();
    int key = 0;
    Tuple temp = null;
    
    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      t.tupleCopy(temp);
      
      try {
	key = t.getIntFld(3);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btf.insert(new IntegerKey(key), rid); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scan.closescan();
    
    System.out.println("BTreeIndex file created successfully.\n"); 
    
    FldSpec[] projlist = new FldSpec[4];
    RelSpec rel = new RelSpec(RelSpec.outer); 
    projlist[0] = new FldSpec(rel, 1);
    projlist[1] = new FldSpec(rel, 2);
    projlist[2] = new FldSpec(rel, 3);
    projlist[3] = new FldSpec(rel, 4);
    
    // conditions
    CondExpr[] expr = new CondExpr[3]; 
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopGE);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrInteger);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
    expr[0].operand2.integer = 100;
    expr[0].next = null;
    expr[1] = new CondExpr();
    expr[1].op = new AttrOperator(AttrOperator.aopLE);
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
    expr[1].operand2.integer = 900;
    expr[1].next = null;
    expr[2] = null;

    // start index scan
    IndexScan iscan = null;
    try {
      iscan = new IndexScan(new IndexType(IndexType.B_Index), "test3.in", "BTIndex", attrType, attrSize, 4, 4, projlist, expr, 3, false);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    

    t = null;
    int iout = 0;
    int ival = 100; // low key
    
    try {
      t = iscan.get_next();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace(); 
    }

    while (t != null) {
      try {
	iout = t.getIntFld(3);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      if (iout < ival) {
	System.err.println("count = " + count + " iout = " + iout + " ival = " + ival);
	
	System.err.println("Test3 -- OOPS! index scan not in sorted order");
	status = FAIL;
	break; 
      }
      else if (iout > 900) {
	System.err.println("Test 3 -- OOPS! index scan passed high key");
	status = FAIL;
	break;
      }
      
      ival = iout;
      
      try {
	t = iscan.get_next();
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    if (status) {
      System.err.println("Test3 -- Index scan on int key OK\n");
    }

    // clean up
    try {
      iscan.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    System.err.println("------------------- TEST 3 completed ---------------------\n");
        
    return status;
  }
    
  protected boolean test4()
  {
    return true;
  }
    
  protected boolean test5()
  {
    return true;
  }
    
  protected boolean test6()
  {
    return true;
  }
    
  protected String testName()
  {
    return "Index";
  }
}

public class IndexTest
{
  public static void main(String argv[])
  {
    boolean indexstatus;

    IndexDriver indext = new IndexDriver();

    indexstatus = indext.runTests();
    if (indexstatus != true) {
      System.out.println("Error ocurred during index tests");
    }
    else {
      System.out.println("Index tests completed successfully");
    }
  }
}

