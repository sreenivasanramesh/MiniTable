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
- Type 2: One Btree to index row labels.
- Type 3: One Btree to index column labels.
-  Type 4: One Btree to index column label and row label (combined key) and one Btree to index timestamps.
- Type 5: One Btree to index row label and value (combined key) and one Btree to index timestamps


## Usage

Build the project and then use the following command to enter the CLI.
```
java cmdline/MiniTable.java 
```
The batch insert query is used to insert multiple Maps into a bigTable. A csv with Maps (row, column, timestamp, value) is provided to the batch insert command. The command for batch insert:
``` 
batchinsert PATH_TO_DATAFILE TYPE BIGTABLENAME
```
To query the data you need to pass the index type and order type along with the filters. `NUMBUF` is the number of buffers which will be used during querying. The command for querying:
```
query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
```

## Future Enhancements
- Supporting different storage types in one bigDB.
- Supporting different maps stored of different types in one big table.
- More indexing options, with possibly sorted heap file storage.
- Map insert into the big table. Currently only batch inserts are supported.
- Implementing a join operator.
- Modifying all Minibase internal structures such as directory pages to use Maps instead of Tuples.
- Other sorting options.
