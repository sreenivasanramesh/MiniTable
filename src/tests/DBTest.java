package tests;
//From db_driver.C

import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import global.*;

/** Note that in JAVA, methods can't be overridden to be more private.
    Therefore, the declaration of all private functions are now declared
    protected as opposed to the private type in C++.
*/

class DBDriver extends TestDriver implements GlobalConst {

  private PageId runStart = new PageId();
  private boolean OK = true;
  private boolean FAIL = false;

  public DBDriver () {
    super("dbtest");
  }


  public boolean runTests () {

    System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
   
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
    
    System.out.print ("\n" + "..." + testName() + " tests ");
    System.out.print (_pass==OK ? "completely successfully" : "failed");
    System.out.print (".\n\n");
    
    return _pass;
  }
  
  protected boolean test1 () {

    SystemDefs sysdef = new SystemDefs( dbpath, 8193,  100, "Clock" );

    System.out.print("\n  Test 1 creates a new database and does " + 
		       "some tests of normal operations:\n");
    
    boolean status = OK;
    
    PageId pgid = new PageId();
    pgid.pid = 0;
    
    System.out.print ("  - Add some file entries\n");
    
    for ( int i=0; i < 6 && status == OK; ++i ) {
      String name = "file" + i; 
      try {
	SystemDefs.JavabaseDB.allocate_page(pgid, 1);
      }
      catch (java.io.IOException e){
	System.err.println("IOerror: " + e);
	status = FAIL;
	System.err.println ("*** Could not allocate a page");
	e.printStackTrace();
      }
      
      catch ( Exception e ) {
	status = FAIL;
	System.err.println ("*** Could not allocate a page");
	e.printStackTrace();
      }
      
      if ( status == OK ) {
	try {
	  SystemDefs.JavabaseDB.add_file_entry(name,pgid);
	}
	catch (Exception e) { 
	  status = FAIL;
	  System.err.print("*** Could not add file entry "+name+"\n");
	  e.printStackTrace();
	}
      }
    }
    
    if ( status == OK ) {
      System.out.print ("  - Allocate a run of pages\n");
      try {
        SystemDefs.JavabaseDB.allocate_page( runStart, 30 );
      }
      catch ( java.io.IOException e){
          status = FAIL;
	  System.err.println ("*** Could not allocate a run of page");
          e.printStackTrace();
      }
      catch ( Exception e ) {
	status = FAIL;
	System.err.println ("*** Could not allocate a run of page");
	e.printStackTrace();
      }
    }


    if ( status == OK ) {
      System.out.print ("  - Write something on some of them\n");
      for ( int i=0; i<20 && status == OK; ++i ) {

	String writeStr = "A" + i;
	
	byte [] data = new byte[2*writeStr.length()]; //leave enough space
	try {
	  Convert.setStrValue (writeStr, 0, data);
	}
	catch (IOException e) {
	  System.err.println (""+e);
	  status = FAIL;
	  e.printStackTrace();
	}
	Page pg = new Page(data);

	try {
          SystemDefs.JavabaseDB.write_page(new PageId(runStart.pid+i), pg);
	}
	catch (Exception e) {
	  status = FAIL;
	  System.err.print("*** Error writing to page " +
			   (runStart.pid+i) + "\n");
	  e.printStackTrace();
	}
      }
    }
    
    if ( status == OK ) {
      System.out.print ("  - Deallocate the rest of them\n");
      try {
	SystemDefs.JavabaseDB.deallocate_page(new PageId(runStart.pid+20),10); 
      }
      catch (java.io.IOException e){
	status = FAIL;
	System.err.print ("*** Error deallocating pages\n");
	e.printStackTrace();
      }	
      
      catch (Exception e) {
	status = FAIL;
	System.err.print ("*** Error deallocating pages\n");
	e.printStackTrace();
      }
    }
    
    if ( status == OK )
      System.out.print ("  Test 1 completed successfully.\n");
    
    return status;  
    
  }  
  protected boolean test2 () {
    
    System.out.print ("\n  Test 2 opens the database created in " +
			"test 1 and does some further tests:\n");

    boolean status = OK;

    PageId pgid = new PageId();
    pgid.pid = 0;
    
    
    System.out.print ("  - Delete some of the file entries\n");
    for ( int i=0; i<3 && status == OK; ++i ) {
      String name = "file" + i;
      try {
        SystemDefs.JavabaseDB.delete_file_entry(name);
      }
      catch (Exception e) {
	status = FAIL;
	System.err.print ("*** Could not delete file entry " + name + "\n");
	e.printStackTrace();
      }
    }

    if ( status == OK ) {
      System.out.print ("  - Look up file entries that should " +
			  "still be there\n");
      for ( int i=3; i<6 && status == OK; ++i ) {
	String name = "file" + i;
	try {
	  pgid = SystemDefs.JavabaseDB.get_file_entry(name);
	}
	catch (Exception e) {
	  status = FAIL;
	  System.err.print ("*** Could not find file entry " + name + "\n");
	  e.printStackTrace();
	}
      }
    }
    
    if ( status == OK ) {
      System.out.print("  - Read stuff back from pages we wrote in test 1\n");
      
      
      for ( int i=0; i<20 && status == OK; ++i ) {
	Page pg = new Page();
	try {
	  SystemDefs.JavabaseDB.read_page(new PageId(runStart.pid+i),pg);
	}
	catch (Exception e) {
	  status = FAIL;
	  System.err.print ("*** Error reading from page " 
			      + (runStart.pid+i) + "\n");
          e.printStackTrace();
	}

	String testStr = "A" + i;
	String readStr = new String();
	try {
	  readStr = Convert.getStrValue(0,pg.getpage(),2*testStr.length()); 
	}
	catch (IOException e) {
	  System.err.println (""+e);
	  status = FAIL;
	  e.printStackTrace();
	}

	if (readStr.equals(testStr) != true) {
	  status = FAIL;
	  System.err.print ("*** Data read does not match what " +
			      "was written on page " +
			      (runStart.pid+i) + "\n");
	
	}
      }
    }
    
    if ( status == OK )
      System.out.print ("  Test 2 completed successfully.\n");
    
    //final cleaning up before we leave the test
    PageId tmp = new PageId();
    tmp.pid =3;
    try {
      SystemDefs.JavabaseDB.deallocate_page(tmp, 26 );
    }  
    
    catch (Exception e){
      System.err.print ("*** Error deallocating pages\n");
      e.printStackTrace();
    }
    
    return status; 
  }

  protected boolean test3 () {

    System.out.print ("\n  Test 3 tests for some error conditions:\n");

    boolean status = OK;
    PageId pgid = new PageId(0);
    
    if ( status == OK ) {
      System.out.print ("  - Look up a deleted file entry\n");
      try {
        pgid = SystemDefs.JavabaseDB.get_file_entry("file1");
	if(pgid == null) { //no exception should be thrown in this case
	  status = FAIL;
	  System.err.println ("**** Looking up a deleted file entry");
	  System.out.println ("  --> Failed as expected \n");
	}
      }
      
      catch (Exception e) {
	e.printStackTrace();
      }
      
      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
	
      }
      else { 
	status = OK; 
      }
    }
    
    if ( status == OK ) {
      System.out.print ("  - Try to delete a deleted entry again\n");
      try {
	SystemDefs.JavabaseDB.delete_file_entry("file1");
      }
      catch (FileEntryNotFoundException e) {
	System.err.println ("**** Delete a deleted file entry again");
	System.out.println ("  --> Failed as expected \n");
	status = FAIL;
      }
      catch (Exception e) {e.printStackTrace();}

      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK; 
      }
    }

    if ( status == OK ) {
      System.out.print ("  - Try to delete a nonexistent file entry\n");
      try {
        SystemDefs.JavabaseDB.delete_file_entry("blargle");
      }
      catch (FileEntryNotFoundException e) {
	System.err.println ("**** Deleting a nonexistent file entry" );
	System.out.println ("  --> Failed as expected \n");
	status = FAIL;
      }
      catch (Exception e) {e.printStackTrace();}

      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK; 
      }
    }

    if ( status == OK ) {
      System.out.print ("  - Look up a nonexistent file entry\n");
      try {
        pgid = SystemDefs.JavabaseDB.get_file_entry("blargle");
	if(pgid == null) {
	  System.err.println ( "**** Looking up a nonexistent file entry");
	  System.out.println ("  --> Failed as expected \n");
	  status = FAIL;
	}
      }
      
      catch (Exception e) {e.printStackTrace();}
      
      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK; 
      }
    }

    if ( status == OK ) {
      System.out.print ("  - Try to add a file entry that's already there\n");
      try {
        SystemDefs.JavabaseDB.add_file_entry("file3",runStart);
      }
      catch (DuplicateEntryException e) {
        System.err.println ("**** Adding a duplicate file entry");
	System.out.println ("  --> Failed as expected \n");
	status = FAIL;
      }
      catch (Exception e) {e.printStackTrace();}
      
      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK; 
      }
    }

   
    if ( status == OK ) {
      System.out.print ("  - Try to add a file entry whose name is too long\n");
      //creat a byte array that is big enough to fail the test
      char [] data =  new char [MAX_NAME + 5];
      for (int i = 0; i < MAX_NAME+5; i++) {
	data[i] = 'x';
      }
	
      String name = new String (data); //make it big,just for the heck of it
      try {
        SystemDefs.JavabaseDB.add_file_entry( name, new PageId(0));
      }
      catch (FileNameTooLongException e) {
        System.err.println ( "**** Adding a file entry with too long a name" );
	System.out.println ("  --> Failed as expected \n");
	status = FAIL;
      }
      catch (Exception e) {e.printStackTrace();}
      
      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK;
      }
    }

    if ( status == OK ) {
      System.out.print ("  - Try to allocate a run of pages that's too long \n");
      try {
        pgid = new PageId();
	SystemDefs.JavabaseDB.allocate_page( pgid,9000 ); 
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
      }
      
      catch (OutOfSpaceException e) {
        System.err.println ( "**** Allocating a run that's too long" );
	System.out.println ("  --> Failed as expected \n");
	status = FAIL;
      }
      catch (Exception e) {e.printStackTrace();}
      
      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK;
      }
    }

    if ( status == OK ) {
      System.out.print ("  - Try to allocate a negative run of pages \n");
      try {
        SystemDefs.JavabaseDB.allocate_page( pgid, -10 ); //made up 1 to test -10
      }
      
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
	}

      catch (InvalidRunSizeException e) {
        System.err.println ( "**** Allocating a negative run" );
	System.out.println ("  --> Failed as expected \n");
	status = FAIL;
      }
      catch (Exception e) {e.printStackTrace();}
      
      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK;
      }
    }

    if ( status == OK ) {
      System.out.print ("  - Try to deallocate a negative run of pages \n");
      try {
        SystemDefs.JavabaseDB.deallocate_page( pgid, -10 ); //made up 1 to test -10
      }
	catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
	}

      catch (InvalidRunSizeException e) { 
        System.err.println ( "**** Deallocating a negative run" );
	System.out.println ("  --> Failed as expected \n");
	status = FAIL;
      }
      catch (Exception e) {e.printStackTrace();}
      
      if (status == OK) { 
	status = FAIL; 
	System.err.println ("The expected exception was not thrown\n");
      }
      else { 
	status = OK;
      }
    }

    if ( status == OK )
      System.out.print ("  Test 3 completed successfully.\n");

    return status;
  }
  
  protected boolean test4 ()  {
    
    boolean status = OK;
    
    System.out.print ("\n  Test 4 tests some boundary conditions.\n" +
		      "    (These tests are very " +
		      "implementation-specific.)\n");
    
    // We create a new database that's big enough to require 2 pages to hold
    // its space map.
    int bits_per_page = MAX_SPACE * 8;
    int dbsize = bits_per_page + 1;

    PageId pgid = new PageId(0);

    System.out.print ("  - Make sure no pages are pinned\n");
    if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers() ) {
      System.err.print("**1* The disk space manager has left " +
		       "pages pinned\n");
      status = FAIL;
    }
    
    if ( status == OK ) {
      System.out.print ("  - Allocate all pages remaining after " +
			"DB overhead is accounted for\n");
      try {
        SystemDefs.JavabaseDB.allocate_page( pgid, dbsize-3 );
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
	e.printStackTrace();
      }
      
      catch (Exception e){
	status = FAIL;
	e.printStackTrace();
	System.err.print("*** Too little space available: could not " +
			 "allocate " + (dbsize - 3) + " pages\n");
      }
      
      if (status == OK) {
	if ( pgid.pid != 3 ) {
	  status = FAIL;
	  System.err.print("*** Expected the first page allocated to " +
			   "be page 3\n");
	}
	else if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
		 != SystemDefs.JavabaseBM.getNumBuffers() ) {
	  status = FAIL;
	  System.err.print("*2** The disk space manager has left pages pinned\n");
	}
	else {
	  System.out.print ("  - Attempt to allocate one more page\n");
	  try {
	    SystemDefs.JavabaseDB.allocate_page( pgid, 1 );
	  }
	  catch (java.io.IOException e)
	    {
	      System.err.println(" IOerror: " + e);
	      e.printStackTrace();
	    }
	  
	  catch (OutOfSpaceException e) { 
	    status = FAIL;
	    System.err.println ("**** Allocating one additional page" );
	    System.out.println ("  --> Failed as expected \n");
	  }
	  catch (Exception e) {e.printStackTrace();}
	  
	  if (status == OK) { 
	    status = FAIL; 
	    System.err.println ("The expected exception was not thrown\n");
	  }
	  else { 
	    status = OK; 
	  }
	}
      }
    }

    PageId pd = new PageId(3);
    if ( status == OK ) {
      System.out.print ("  - Free some of the allocated pages\n");
      try {
	SystemDefs.JavabaseDB.deallocate_page(pd, 7 );  //free page 3-9
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
        status = FAIL;
        System.err.println ("*** Error deallocating pages\n");
      }
      
      catch (Exception e) {
	status = FAIL;
	System.err.println ("*** Error deallocating pages\n");
      }
    }
      
    PageId pd2 = new PageId(33);
    if ( status == OK ) {
      try {
	SystemDefs.JavabaseDB.deallocate_page(pd2, 8 ); //free page 33-40
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
	status = FAIL;
	System.err.print ("*** Error deallocating pages\n");
      }
      
      catch (Exception e) {
	status = FAIL;
	System.err.print ("*** Error deallocating pages\n");
      }
    }
    
    if ( status == OK ) {
      System.out.print ("  - Allocate some of the just-freed pages\n");
      try {
	SystemDefs.JavabaseDB.allocate_page( pgid, 8 );      //pages 33-40 added
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
      }
      
      catch (Exception e) {
	status = FAIL;
	System.err.print ("*** Could not allocate pages\n");
      }
    } 
    
    if ( status == OK && pgid.pid != 33 ) {
      status = FAIL;
      System.err.print ("*** Allocated wrong run of pages\n");
    }
    
    PageId pg11 = new PageId(11);
    
    if ( status == OK ) {
      System.out.print ("  - Free two continued run of the allocated pages\n");
      try {
	SystemDefs.JavabaseDB.deallocate_page(pg11, 7 );  //deallo pages 11-17
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
	status = FAIL;
	System.err.println ("*** Error deallocating pages\n");
      }
      
      catch (Exception e) {
	status = FAIL;
	System.err.println ("*** Error deallocating pages\n");
	e.printStackTrace();
      }
      
      PageId pg18 = new PageId(18);
      if ( status == OK ) {
	try {
	  SystemDefs.JavabaseDB.deallocate_page(pg18, 11 );  //deallocate 18-28
	}
	catch (java.io.IOException e){
	  System.err.println(" IOerror: " + e);
	  status = FAIL;
	  System.err.print ("*** Error deallocating pages\n");
	}
	
	catch (Exception e) {
	  status = FAIL;
	  System.err.print ("*** Error deallocating pages\n");
	  e.printStackTrace();
	}
      }
    }
    
    if ( status == OK ) {
      System.out.println ("  - Allocate back number of pages equal " + 
			  "to the just freed pages\n");
      try {
	SystemDefs.JavabaseDB.allocate_page( pgid, 18 ); //reallocate from 11-28
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
	status = FAIL;
	System.err.print ("*** Error deallocating pages\n");
      }
      
      catch (Exception e) {
	status = FAIL;
	System.err.print ("*** Could not allocate pages\n");
	e.printStackTrace();
      }

      if ( status == OK && pgid.pid != 11 ) {
	status = FAIL;
	System.err.print ("*** Allocated wrong run of pages\n");
      }
    }
    
    //Delete some leftover file entries
    for ( int i=3; i<6 && status == OK; ++i ) {
      String name = "file" + i;
      try {
        SystemDefs.JavabaseDB.delete_file_entry(name);
      }
      catch (Exception e) {
        status = FAIL;
        System.err.print ("*** Could not delete file entry " + name + "\n");
	e.printStackTrace();
      }
    }
    
    if ( status == OK ) {
      System.out.print ("  - Add enough file entries that the directory " +
			"must surpass a page\n");
      
      // This over-counts, but uses only public info.
      int count = MAX_SPACE / MAX_NAME + 1;   //=21
      
      for ( int i=0; i < count && status == OK; ++i ) {
	String name = "file" + i;
	
	// Set every file's first page to be page 0, which doesn't
	// cause an error.
	try {
	  SystemDefs.JavabaseDB.add_file_entry(name, new PageId(0));
	}
	catch (Exception e) {
	  status = FAIL;
	  System.err.print ("*** Could not add file " + i + "\n");
          e.printStackTrace();
	}
      }
    }
    
    if ( status == OK ) {
      
      System.out.print ("  - Make sure that the directory has " +
			"taken up an extra page: try to\n" +
			"    allocate more pages than should be available\n");
      
      // There should only be 6 pages available.
      try {
	SystemDefs.JavabaseDB.allocate_page( pgid,7 );         
      }
      
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
      }
      
      catch (OutOfSpaceException e) { 
	status = FAIL;
	System.err.println ("**** Allocating more pages than are now available");
	System.out.println ("   --> Failed as expected \n");
      }
      catch (Exception e) {e.printStackTrace();} //All other exceptions 
      
      if (status == OK) {
	status = FAIL;
	System.err.println ("The expected exception was not thrown \n");
      }
      else {
	status = OK;
      }
      
      if (status == OK) {
	try {
	  SystemDefs.JavabaseDB.allocate_page( pgid, 6 );     // Should work.
	}
	catch (java.io.IOException e){
	  System.err.println(" IOerror: " + e);
	  e.printStackTrace();
	}
	
	catch (Exception e) {
	  status = FAIL;
	  System.err.print ("*** But allocating the number that " +
			      "should be available failed.\n");
	  e.printStackTrace();
	}
      }




    }
    
    
    if ( status == OK ) {
      System.out.print ("  - At this point, all pages should be claimed.  " +
			"Try to allocateone more.\n");
      try {
	SystemDefs.JavabaseDB.allocate_page(pgid);      //allocate 1 page
      }
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
      }
      
      catch (OutOfSpaceException e) {
	status = FAIL;
	System.err.println ("**** Allocating one more page than there is" );
	System.out.println ("   --> Failed as expected \n");
      }
      catch (Exception e) {e.printStackTrace();}
      
      if (status == OK) {
	status = FAIL;
	System.err.println ("The expected exception was not thrown \n");
      }
      else {
	status = OK;
      }
    }
    
    if ( status == OK ) {
      System.out.print ("  - Free the last two pages: this tests a boundary " +
			  "condition in the space map.\n");
      try {
	SystemDefs.JavabaseDB.deallocate_page( new PageId(dbsize-2), 2 );
      }
      
      catch (java.io.IOException e){
	System.err.println(" IOerror: " + e);
	e.printStackTrace();
      }
      
      catch (Exception e) {
	status = FAIL;
	System.err.print ("*** Did not work.\n");
	e.printStackTrace();
      }
      if ( status == OK && 
	   SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers() ) {
	System.err.print ("*** The disk space manager has left pages pinned\n");
	status = FAIL;
      }
    }
    
    if ( status == OK ) {
      System.out.print ("  Test 4 completed successfully.\n");
    }
    
    return status; 
  }
  
    protected boolean test5 () {
      
    return true;
  }

  protected boolean test6 () {

    return true;
  }

  protected String testName () {
   
    return "Disk Space Management";
  }

  protected boolean runAllTests (){
    
    boolean _passAll = OK;
    
    if (!test1()) { _passAll = FAIL; }
    if (!test2()) { _passAll = FAIL; }
    if (!test3()) { _passAll = FAIL; }
    if (!test4()) { _passAll = FAIL; }
    if (!test5()) { _passAll = FAIL; }
    if (!test6()) { _passAll = FAIL; }
   try{
    SystemDefs.JavabaseDB.DBDestroy();
   
   }
   catch (IOException e){
    System.err.println(" DB already destroyed");
   }
    return _passAll;
  }
}

public class DBTest {

   public static void main (String argv[]) {

     DBDriver dbt = new DBDriver();
     boolean dbstatus;

     dbstatus = dbt.runTests();

     if (dbstatus != true) {
       System.err.println ("Error encountered during buffer manager tests:\n");
       Runtime.getRuntime().exit(1);
     }

     Runtime.getRuntime().exit(0);
   }
}

