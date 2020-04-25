# MiniTable - A BigTable/HBase implementation using Minibase


## Implementation

We modify the Minibase distribution to support a wide-columnar store similar to BigTable. Minibase is a relational DBMS, which stores data in the form of Tuples. We extend Minibase with a new Map construct which has 4 fields.

```(row : string, column : string, time : int) → (value : string)```

We have also added support for versioning, where we maintain the 3 most recent Maps.


Similar to the heap.Scan class available in Minibase, we provide BigT.Stream which initializes a stream of maps which can are filtered and ordered by the orderType. Currently supported filter are :
- \* : star filter returns everything
-  single values
-  range values specified within brackets (eg:[Arizona,California] )

If orderType is
- 1 results are first ordered by row label, then column label, then time stamp.
- 2 results are first ordered by column label, then row label, then time stamp.
- 3 results are first ordered by row label, then time stamp.
- 4 results are first ordered by column label, then time stamp.
- 5 results are ordered by time stamp.

We have also extended the diskmgr package, which created and maintains our Btree based index files to organize the data. Currently supported index types are:
- Type 1: No index.
- Type 2: One Btree to index row labels, maps are row label sorted (in increasing order).
- Type 3: One Btree to index column labels, maps are column label sorted (in increasing order).
-  Type 4: One Btree to index column label and row label (combined key) and one Btree to index timestamps; maps are sorted on combined key (in increasing order).
- Type 5: One Btree to index row label and value (combined key) and one Btree to index timestamps; maps are sorted on combined key (in increasing order).


### New changes:

We have also modified the bigDB class such that the constructor does not take type as input. 
In other words, in the same bigDB database there may be data stored according to different storage types.

The bigT class is also modified such that class such that the bigt method does not take type as input. 
In other words, in the same bigT table there may be maps stored according to different storage types.


## Usage

Build the project and then use the following command to enter the CLI.
```
java cmdline/MiniTable.java 
```
The batch insert query is used to insert multiple Maps into a bigTable. A csv with Maps (row, column, timestamp, value) is provided to the batch insert command. The command for batch insert:
``` 
batchinsert PATH_TO_DATAFILE TYPE BIGTABLENAME NUMBUF
```
To insert a single map into the table use the mapinsert command:
```
mapinsert ROW_LABEL COLUMN_LABEK VALUE TIMESTAMP TYPE BIGTABLENAME NUMBUF
```
To query the data you need to pass the order type along with the filters. `NUMBUF` is the number of buffers which will be used during querying. The command for querying:
```
query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF```
```
We have also implemented a simple join operator to understand how it works. 
```
RowJoin(int amt of mem, Stream leftStream, java.lang.String RightBigTName, attrString ColumnName)
amt_of_mem - IN PAGES
leftStream - a stream for the left data source
RightBigTName - name of the BigTable at the right side of the join ColumnName - condition to match the column labels
```
The output is a stream of maps corresponding to a BigT consisting of the maps of the matching rows based on the given conditions, such that 
- Two rows match only if they have the same column and the most recent values for the two columns are the same.
- The resulting rowlabel is the concatenation of the two input rowlabels, seperated with a “:”
- The resulting row has all the columnlabels of the two input rows, except for the joined column which
occurs only once in the bigtable – and only with the most recent three values.

The syntax for row join is as follows:
```aidl
rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER NUMBUF
```
the program will access the database to rowjoin the two bigtables and create a new type 1 big table with the given table name. Minibase will use at most NUMBUF buffer pages to run the query.

We have also implemented an external row sort operator. The operator that results in a type 1 BigT in which the rows are sorted (in non- decreasing order) according to the most recent values for the given column label.
Tht syntax for row sort is as follows:
```
rowsort INBTNAME OUTBTNAME COLUMNNAME NUMBUF
```

The getCounts command will return the numbers of maps, distinct row labels, and distinct column labels. 
```
getCounts NUMBUF
```
## Future Enhancements
- [x] Supporting different storage types in one bigDB.
- [x] Supporting different maps stored of different types in one big table.
- [x] More indexing options, with possibly sorted heap file storage.
- [x] Implementing a join operator.
- Modifying all Minibase internal structures such as directory pages to use Maps instead of Tuples.

