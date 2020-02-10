package tests;

import java.io.*;
import java.lang.*;
import java.util.*;
import global.*;
import iterator.*;
import heap.*;

//Set set the structures needed
//enum Order{UNSORT,SORT};
class Order {
  static final int UNSORT = 0;
  static final int SORT   = 1;
  int order;
  
  public Order(int _order) {
    order = _order;
  }
}
  
/*
  struct Group{
  int len;                 // The num of tuple in the group.
  int count;               // The count for correct answers in the group.
  Order order;             // order of tuples in the group
  int   mark[Max_answer];  // 1:tuple correct. 0:not yet checked
  Tuple * mytuple[Max_answer]; // The answer in the group.
  };
*/

class Group {
  static int Max_answer = 15;
  int len;
  int count;
  Order order;
  int [] mark;
  Tuple [] mytuple;

  public Group () {
    mark = new int[Max_answer];
    mytuple = new Tuple[Max_answer];
  }
}

/*
  struct TupleList{
  Tuple *       tuple;
  TupleList *   next;
  };
*/

class TupleList {
  Tuple tuple;
  TupleList next;

  public TupleList(){};
}
 
// set up answers for each query.
class S1 {
  public String sname;
  public String date;
  
  public S1 (String _sname, String _date) {
    sname = _sname;
    date = _date;
  }
}

class S2{
  public String sname;
  public S2 (String _sname) {
    sname = _sname;
  }
}
    
class S5{
  String  sname;
  int     rating;
  float   age;
  public S5 (String _sname, int _rating, float _age) {
    sname = _sname;
    rating = _rating;
    age = _age;
  }
}

public class QueryCheck {

  public static final int Max_group_num = 5;
  public static final int Max_answer = 15;

  private AttrType [] types; 
  private short[] sizes;// sizes of attributes in answer tuple
  private short    columnum;     // number of attributes in answer tuple
  private int      curGroup;     // current group number
  private int      tuplenum;     // total number of answer tuples
  private int      groupnum;     // number of groups in answer tuples
  private Order    grouporder;   // order of groups
  
  private Group [] mygroup;

  // group mark, 1: checked already, 0: not checked
  private int gmark[];    
  
  private int      total;        // total number of correct answers
  private int      G_O_flag;     // error flag for group order wrong
  // error flag for tuple order wrong
  private int[] T_O_flag;  
  
  private TupleList missing;
  private TupleList extra;

  //set up answers for each query
  Vector Q1result = new Vector();
  Vector Q2result = new Vector();
  Vector Q3result = new Vector();
  Vector Q4result = new Vector();
  Vector Q5result = new Vector();
  Vector Q6result = new Vector();
  Vector Q7result = new Vector();



  /** Constructor
   */
  public QueryCheck(int q_index) {

    types = new AttrType[10];    
    sizes = new short[10];// sizes of attributes in answer tuple
    mygroup = new Group[Max_group_num];
    gmark = new int[Max_group_num];    
    T_O_flag = new int[Max_group_num];  
    missing = new TupleList();
    extra = new TupleList();
    
    Q1result.addElement (new S1("Mike Carey", "05/10/95"));
    Q1result.addElement (new S1("David Dewitt", "05/11/95"));
    Q1result.addElement (new S1("Jeff Naughton", "05/12/95"));
    
   
    Q2result.addElement (new S2("David Dewitt"));
    Q2result.addElement (new S2("Mike Carey"));
    Q2result.addElement (new S2("Raghu Ramakrishnan"));
    Q2result.addElement (new S2("Yannis Ioannidis"));

   
    Q3result.addElement (new S2("Mike Carey"));
    Q3result.addElement (new S2("Mike Carey"));
    Q3result.addElement (new S2("Mike Carey"));    
    Q3result.addElement (new S2("David Dewitt"));
    Q3result.addElement (new S2("David Dewitt"));
    Q3result.addElement (new S2("Jeff Naughton"));
    Q3result.addElement (new S2("Miron Livny"));
    Q3result.addElement (new S2("Yannis Ioannidis"));
    Q3result.addElement (new S2("Raghu Ramakrishnan"));
    Q3result.addElement (new S2("Raghu Ramakrishnan"));

  
    Q4result.addElement (new S2("David Dewitt"));
    Q4result.addElement (new S2("Jeff Naughton"));
    Q4result.addElement (new S2("Mike Carey"));
    Q4result.addElement (new S2("Miron Livny"));
    Q4result.addElement (new S2("Raghu Ramakrishnan"));
    Q4result.addElement (new S2("Yannis Ioannidis"));

   
    Q5result.addElement (new S5("Mike Carey", 9, (float)40.3));
    Q5result.addElement (new S5("Mike Carey", 9, (float)40.3));
    Q5result.addElement (new S5("Mike Carey", 9, (float)40.3));
    Q5result.addElement (new S5("David Dewitt",10, (float)47.2));
    Q5result.addElement (new S5("David Dewitt",10, (float)47.2));
    Q5result.addElement (new S5("Jeff Naughton", 5,(float) 35.0));
    Q5result.addElement (new S5("Yannis Ioannidis", 8, (float)40.2));

    Q6result.addElement (new S2("David Dewitt"));
    Q6result.addElement (new S2("Mike Carey"));
    Q6result.addElement (new S2("Raghu Ramakrishnan"));
    Q6result.addElement (new S2("Yannis Ioannidis"));
   
    
   
   

    //more initializing
    for (int i = 0; i < Max_group_num; i++) {
      mygroup[i] = new Group();
    }
    
    //NOW the checking part
    switch(q_index) {
      
    case 1:
      types[0] = new AttrType(AttrType.attrString);    // atrrtype array.
      types[1] = new AttrType(AttrType.attrString);
      sizes[0] = (short)25 ;                  // string length. 
      sizes[1] = (short)10;
      columnum = (short)2;                    // colum number.
      groupnum = 1;                    // group number.
      tuplenum = 3;                     // the total number of answer.
      grouporder = new Order(Order.SORT);
      mygroup[0].len = 3;              // each group length.
      mygroup[0].count = 0;           // count for correct answers
      mygroup[0].order = new Order(Order.UNSORT); // the tuple in group is sorted or not
      for(int i=0; i<mygroup[0].len;i++)  {// set tuple value. 
	try {
          mygroup[0].mytuple[i] = new Tuple();
          mygroup[0].mytuple[i].setHdr(columnum, types, sizes); 
          mygroup[0].mytuple[i].setStrFld(1,((S1)Q1result.elementAt(i)).sname);  
          mygroup[0].mytuple[i].setStrFld(2,((S1)Q1result.elementAt(i)).date);  
	}
	catch (Exception e) {
	  System.err.println ("**** Error setting up the tuples");
	}
      }
      break;
      
    case 2:
      types[0] = new AttrType(AttrType.attrString);
      sizes[0] = (short)25;
      columnum = (short)1;
      groupnum = 1;
      tuplenum =4;
      grouporder = new Order(Order.SORT);
      mygroup[0].len = 4;
      mygroup[0].count = 0;           // count for correct answers
      mygroup[0].order = new Order(Order.SORT);
      for(int i=0; i<mygroup[0].len;i++)  {// set tuple value.
        try {
          mygroup[0].mytuple[i] = new Tuple();
          mygroup[0].mytuple[i].setHdr(columnum, types, sizes);
          mygroup[0].mytuple[i].setStrFld(1,((S2)Q2result.elementAt(i)).sname);
	}
	catch (Exception e) {
	  System.err.println ("**** Error setting up the tuples");
	}
      }
      break;
      
    case 3:
      types[0] = new AttrType(AttrType.attrString);
        sizes[0] = (short)25;
        columnum = (short)1;
        groupnum = 1;
        tuplenum = 10;
        grouporder = new Order(Order.SORT);
        mygroup[0].len = 10;
        mygroup[0].count = 0;           // count for correct answers
        mygroup[0].order = new Order(Order.UNSORT);
        for(int i=0; i<mygroup[0].len;i++) {
	  try {
	    mygroup[0].mytuple[i] = new Tuple();
	    mygroup[0].mytuple[i].setHdr(columnum, types, sizes);
	    mygroup[0].mytuple[i].setStrFld(1,((S2)Q3result.elementAt(i)).sname);
	  }
	  catch (Exception e) {
	    System.err.println ("**** Error setting up the tuples");
	  }
	}
        break;

    case 4:
        types[0] = new AttrType(AttrType.attrString);
        sizes[0] = (short)25;
        columnum = (short)1;
        groupnum = 1;
        tuplenum = 6;
        grouporder = new Order(Order.UNSORT);
        mygroup[0].len = 6;
        mygroup[0].count = 0;           // count for correct answers
        mygroup[0].order = new Order(Order.UNSORT);
        for(int i=0; i<mygroup[0].len;i++)  {// set tuple value.
	  try {
	    mygroup[0].mytuple[i] = new Tuple();
	    mygroup[0].mytuple[i].setHdr(columnum, types, sizes);
	    mygroup[0].mytuple[i].setStrFld(1,((S2)Q4result.elementAt(i)).sname);
	  }
	  catch (Exception e) {
	    System.err.println ("**** Error setting up the tuples");
	  }
	}
        break;

    case 5:
        types[0] = new AttrType(AttrType.attrString);
        types[1] = new AttrType(AttrType.attrInteger);
        types[2] = new AttrType(AttrType.attrReal);
        sizes[0] = (short)25;
        columnum = (short)3;
        groupnum = 1;
        tuplenum = 7;
        grouporder = new Order(Order.SORT);
        mygroup[0].len = 7;
        mygroup[0].count = 0;           // count for correct answers
        mygroup[0].order = new Order(Order.UNSORT);
        for(int i=0; i<mygroup[0].len;i++)  {// set tuple value.
	  try {
	    mygroup[0].mytuple[i] = new Tuple();
	    mygroup[0].mytuple[i].setHdr(columnum, types, sizes);
	    mygroup[0].mytuple[i].setStrFld(1,((S5)Q5result.elementAt(i)).sname);
	    mygroup[0].mytuple[i].setIntFld(2,((S5)Q5result.elementAt(i)).rating);
	    mygroup[0].mytuple[i].setFloFld(3,((S5)Q5result.elementAt(i)).age);
	  }
	  catch (Exception e) {
	    System.err.println ("**** Error setting up the tuples");
	  }
	}
        break;

    case 6:
        types[0] = new AttrType(AttrType.attrString);
        sizes[0] = (short)25;
        columnum = (short)1;
        groupnum = 1;
        tuplenum = 4;
        grouporder = new Order(Order.UNSORT);
        mygroup[0].len = 4;
        mygroup[0].count = 0;           // count for correct answers
        mygroup[0].order = new Order(Order.SORT);
        for(int i=0; i<mygroup[0].len;i++) { // set tuple value.
	  try {
            mygroup[0].mytuple[i] = new Tuple();
            mygroup[0].mytuple[i].setHdr(columnum, types, sizes);
            mygroup[0].mytuple[i].setStrFld(1,((S2)Q6result.elementAt(i)).sname);
	  }
	  catch (Exception e) {
	    System.err.println ("**** Error setting up the tuples");
	  }
        }
        break;

    


    default:
      System.out.print ("Before using querycheck, must set "
			+ "up your answer first.\n\n");
    }
    
    for (int i=0; i<Max_group_num; i++) {     
      gmark[i] = 0;           // initilize group mark
      for ( int j=0; j< Max_answer; j++) {// initialize mark.
	mygroup[i].mark[j] = 0;
      }
    }
    
    total = 0;
    curGroup = -1;
    
    G_O_flag = 0;
    for( int j=0; j<Max_group_num; j++ ) {
      T_O_flag[j] = 0;
    }
    
    missing = null;
    extra   = null; 
  }
  
  
  void AddtoList(TupleList list, Tuple t) {        

    TupleList cur = new TupleList();
    cur.tuple = new Tuple();
    try {
      cur.tuple.setHdr(columnum, types, sizes);
    }
    catch (Exception e) {
      System.err.println ("**** Error setting up the tuples");
    }
    TupleCopy(cur.tuple, t, (int)columnum, types); 
    cur.next = list;
    list = cur;
  }
  
  void TupleCopy(Tuple to, Tuple from, int fldnum, AttrType []type) {

    int   temp_i;
    float temp_f;
    String  temp_s;
    
    for(int i=1; i<=fldnum; i++) {
      switch((type[i-1]).attrType) {
      case AttrType.attrInteger:
	try {
	  temp_i = from.getIntFld(i);
	  to = to.setIntFld(i, temp_i);
	  //to.tupleCopy(from);
	}
	catch (Exception e) {
	  System.err.println ("**** Error setting up the tuples");
	}
	break;
      case AttrType.attrReal:
	try {
	  temp_f = from.getFloFld(i);
	  to = to.setFloFld(i, temp_f);
	  //to.tupleCopy(from);
	}
	catch (Exception e) {
	  System.err.println ("**** Error setting up the tuples");
	}
	break;
      case AttrType.attrString:
	try {
	  temp_s = from.getStrFld(i);
	  to = to.setStrFld(i, temp_s);
	  //to.tupleCopy(from);
	}
	catch (Exception e) {
	  System.err.println ("**** Error setting up the tuples");
	}
	break;
      default:
	//error(Don't know what to do with attrSymbol, attrNull--TupleCopy(..);
	break;
      }
    }
  }
  
  public void Check(Tuple t) {
    
    // first find curGroup
    if( curGroup == -1 ) {
      if( grouporder.order == Order.SORT ) {
	if( gmark[0] == 0 ) {
	  curGroup = 0;
	}
	else {
	  AddtoList(extra,t);
	  return;
	}
      }      
      else {   // grouporder == UNSORT
	int temp[] = new int[1];
	temp[0] = -1;
	curGroup = Search(t, temp);
	if( curGroup == -1) {
	  // t not in answer             
	  AddtoList(extra, t);
	  return;
	}
      }
    }
    
    int count = mygroup[curGroup].count;   // shorthand
    
    // in curGroup
    if( mygroup[curGroup].order.order == Order.SORT ) {

      // mygroup[curGroup].mytuple[count].print(types);

      try {
	TupleUtils tUtil = new TupleUtils();
	if(tUtil.Equal(mygroup[curGroup].mytuple[count],t,types,(int)columnum)) {
	  MarkTuple(curGroup, count);
	}

	else {
	  MisMatch(t);
	}
      }
      catch (Exception e) {
	System.err.println (""+e);
	System.err.println ("***** Error comparing the value of tuples");
      }
      return;
    }
    else {   // no order inside curGroup
      
      // look for tuple t inside curGroup
      for( int i=0; i<mygroup[curGroup].len; i++ ) {
	try {
	  TupleUtils tUtil = new TupleUtils();
	  if((mygroup[curGroup].mark[i] == 0) && 
	     (tUtil.Equal(mygroup[curGroup].mytuple[i],t,types,(int)columnum))) {// found
	    MarkTuple(curGroup, i);
	    return;
	  }
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  System.err.println ("***** Error comparing the value of tuples");
	}
      }
      
      // not found
      MisMatch(t);
      
      return;
    }
  }
  
  void MisMatch(Tuple t) {
    int t_num[] = new int[1];
    
    t_num[0] = -1;
    // first look for it in other groups
    int tempGroup = Search(t, t_num);
    
    if( tempGroup == -1 ) {   // t not in answer tuples
      AddtoList(extra, t);
      return;
    }
    else if( tempGroup == curGroup ) {   
      if( mygroup[curGroup].order.order == Order.UNSORT ) {
	// this should not happen
	System.out.print ("*****Tuple in current group, but "
			  + "checking failed to find it.\n\n");
	return;
      }
      else {  // tuple sorted order in curGroup is wrong
	System.out.print ("\n*****Tuples in group " + curGroup 
			  + " should be sorted.\n\n");
	
	// change order to UNSORT to facilitate further checking.
	mygroup[curGroup].order.order = Order.UNSORT;
	
	// set tuple order error flag
	T_O_flag[curGroup] = 1;
	
	MarkTuple(curGroup, t_num[0]);
	
	return;
      }
    }
    else {  // found in another group                
      // if mygroup[curGroup].count == 0, it's probably due to groups are not 
      // sorted when they are suppose to         
      // Leave curGroup open for further checking           
      if( mygroup[curGroup].count != 0 ) {    
	// add remaining tuple in curGroup to missing list
	if( mygroup[curGroup].count < mygroup[curGroup].len ) {
	  System.out.print ("\n*****Group " + curGroup 
			    + " has missing tuples.\n\n");
	  for( int i=0; i<mygroup[curGroup].len; i++) {
	    if( mygroup[curGroup].mark[i] == 0) {
	      AddtoList(missing, mygroup[curGroup].mytuple[i]);
	    }
	  }
	}
        
	// mark the current group
	gmark[curGroup] = 1;
      }           
      
      // now ready to reset curGroup
      if( grouporder.order == Order.SORT ) {
	curGroup ++;
	if( tempGroup != curGroup ) {   
	  System.out.print ("\n*****Group order is wrong.\n\n");
	  
	  // set group order error flag
	  G_O_flag = 1;
	  
	  // change the grouporder to UNSORT to facilitate further checking
	  grouporder.order = Order.UNSORT;
	  curGroup = tempGroup;
	}
      }
      else {  // group not sorted
	curGroup = tempGroup;
      }
        
      // now check the sorting status of the new curGroup
      if(mygroup[curGroup].order.order == Order.UNSORT ) {
	MarkTuple(curGroup, t_num[0]);
      }
      // tuple should be sorted, check whether it's the first tuple of group
      else if(t_num[0] == 0) {
	MarkTuple(curGroup, t_num[0]);
      }
      // tuple in new curGroup not in correct sorted order
      else {
	System.out.print ("\n*****Tuples in group " + curGroup 
			  + " should be sorted.\n\n");
	
	// set tuple sort order error flag
	T_O_flag[curGroup] = 1;
	
	// reset tuple order to UNSORT to facilitate further checking
	mygroup[curGroup].order.order = Order.UNSORT;
	
	MarkTuple(curGroup, t_num[0]);
      }        
      return;
    }
  }
  
  // MarkTuple will mark the tuple in current group 
  void MarkTuple(int groupNum, int tupleNum) {
    mygroup[groupNum].mark[tupleNum] = 1;
    mygroup[groupNum].count ++;
    total ++;
    
    // check to see whether current group is done
    if( mygroup[groupNum].count == mygroup[groupNum].len ) {
      // mark the group
      gmark[groupNum] = 1;
      
      if( grouporder.order == Order.SORT ) {
	curGroup ++;
	if(curGroup >= groupnum) {
	  curGroup = -1;
	}
      }           
      else {
	curGroup = -1;
      }
    }    
    return;
  }   
  
  // Search() will look for a tuple and return the group number 
  // and tuple number if found
  int Search(Tuple t, int [] t_num) {
    for( int i=0; i<groupnum; i++) {
      if(gmark[i] == 0) {
	for( int j=0; j<mygroup[i].len; j++) {
	  try {
	    TupleUtils tUtil = new TupleUtils();
	    if((mygroup[i].mark[j] == 0) && 
	       ( tUtil.Equal(mygroup[i].mytuple[j],t,types,(int)columnum))) {
	      t_num[0] = j;
	      return i;
	    }
	  }
	  catch (Exception e) {
	    System.err.println (""+e);
	    System.err.println ("***** Error comparing the value of tuples");
	  }
	}
      }
    }
    
    // not found
    t_num[0] = -1;
    return -1;
  }
  
  // report the status of the query
  public void report(int querynum) {
    if( total<tuplenum )
      System.out.print ("\n*****Error occured in QueryCheck.\n\n");
    
    TupleList temp;
    
    try {
      if( missing != null ) {
	System.out.print ("\n***The following tuples are missing "
			  + "from your answer:\n");
	temp = missing;
	while(temp != null ) {
	  temp.tuple.print(types);
	  temp = temp.next;
	}
      }
      
      if( extra != null ) {
	System.out.print ("\n***The following tuples from your answer " 
			  + "are incorrect:\n" );
	temp = extra;
	while(temp != null ) {
	  temp.tuple.print(types);
	  temp = temp.next;
	}
      } 
    }
    catch (Exception e) {
      System.err.println (""+e);
      System.err.println ("**** Error printing the tuples out");
    }
      
    if( missing != null || extra != null ) {
      System.out.print ("\nIf you see the same tuples in the " 
			+ "missing list and the extra\n");
      System.out.print ("  list, your tuples are probably not " 
			+ "grouped correctly.\n");
    }
    
    // check group order error flag
    if( G_O_flag != 0 )
      System.out.print ("\n*****Your group ordering is wrong.\n\n");
    
    // check tuple order error flag
    int t_order_error = 0;   
    for( int j=0; j<groupnum; j++) {
      if( T_O_flag[j] == 1) {
	t_order_error =1;
	System.out.print ("\n*****Your tuple order in group " + j 
			  + " is wrong.\n\n");
      }
    }
    
    if( total == tuplenum && missing == null && 
	extra == null && G_O_flag == 0 && t_order_error == 0) {
      System.out.print ( "\nQuery" + querynum +" completed successfully!\n");
      System.out.print ( "*******************Query"+querynum +" finished!!!*****************\n\n");
    }
    return;
  }   
}
