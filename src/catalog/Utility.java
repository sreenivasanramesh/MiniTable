//------------------------------------
// Utility.java
//
// Ning Wang, April 25, 1998
//-------------------------------------

package catalog;

import java.io.*;

import bufmgr.*;
import global.*;
import heap.*;
import diskmgr.*;
import btree.*;

public class Utility implements Catalogglobal {

    // WRAPS DELETE UTILITY IN TX
    void deleteRecordUT(String relation, attrNode item) {
    }

    ;

    // DELETES RECORDS
    void deleteRecUT(String relation, attrNode item) {
    }

    ;

    // DELETES INDEX ENRIES FOR RECORDS
    void deleteRecIndexesUT(String relation, RID rid, Tuple tuple) {
    }

    ;

    // WRAPS INSERT UTILITY  IN TX
    public static void insertRecordUT(String relation, int attrCnt, attrNode[] attrList)
            throws Catalogmissparam,
            Catalogrelexists,
            Catalogdupattrs,
            Catalognomem,
            IOException,
            Catalogioerror,
            Cataloghferror,
            Catalogrelnotfound,
            Catalogindexnotfound,
            Catalogattrnotfound,
            Catalogbadattrcount,
            Catalogattrexists,
            Catalogbadtype,
            Exception {
        insertRecUT(relation, attrCnt, attrList);
    }

    ;


//---------------------------------------------------
// INSERT A RECORD INTO THE DATABASE
// - takes
//   1. relation name
//   2. attribute count
//   3. array of attribute names and values
// - does
//   1. typechecks
//   2. creates tuple
//   3. inserts into datafile
//   4. inserts into each indexfile
//---------------------------------------------------

    public static void insertRecUT(String relation, int attrCnt, attrNode[] attrList)
            throws Catalogmissparam,
            Catalogrelexists,
            Catalogdupattrs,
            Catalognomem,
            IOException,
            Catalogioerror,
            Cataloghferror,
            Catalogrelnotfound,
            Catalogindexnotfound,
            Catalogattrnotfound,
            Catalogbadattrcount,
            Catalogattrexists,
            Catalogbadtype,
            Exception {
        RelDesc relRec = null;
        RID rid = null;
        int status;
        AttrType attrType = new AttrType(AttrType.attrInteger);
        int attrPos = 0;
        int attrLen;
        int attrCount = 0;
        int indexCount = 0;
        int recSize;

        KeyClass key = null;
        int count = 0;
        int intVal = 0;
        float floatVal = (float) 0.0;
        String strVal = null;

        // DELETE FOLLOWING ON RETURN
        AttrDesc[] attrRecs = null;
        IndexDesc[] indexRecs = null;
        Tuple tuple = null;
        String indexName = null;
        BTreeFile btree = null;
        AttrType[] typeArray = null;
        short[] sizeArray = null;
        Heapfile heap = null;

        // GET RELATION

        ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relation, relRec);


        // CHECK FOR VALID NO OF RECORDS

        if (relRec.attrCnt != attrCnt)
            throw new Catalogbadattrcount(null, "CATALOG: Bad Attribute Count!");


        // GET INFO ON ATTRIBUTES

        attrCount = ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relation, attrCount, attrRecs);

        // CHECK ATTRIBUTE LIST

        for (int z = 0; z < attrCnt; z++)
            if (attrRecs[z].attrName.equalsIgnoreCase(attrList[z].attrName) == true)
                throw new Catalogattrexists(null, "Catalog: Attribute Exists!");


        // GET INFO ON INDEXES

        indexCount = ExtendedSystemDefs.MINIBASE_INDCAT.getRelInfo(relation, indexCount, indexRecs);


        // TYPE CHECK RIGHT HERE......Make sure that the values being
        // passed are valid

        for (int i = 0; i < attrCount; i++) {
            switch (attrRecs[i].attrType.attrType) {
                case (AttrType.attrInteger):
                    if (!check_int(attrList[i])) {
                        throw new Catalogbadtype(null, "Catalog: Bad Type!");
                    }
                    break;

                case (AttrType.attrReal):
                    if (!check_float(attrList[i])) {
                        throw new Catalogbadtype(null, "Catalog: Bad Type!");
                    }
                    break;

                case (AttrType.attrString):
                    if (!check_string(attrList[i])) {
                        throw new Catalogbadtype(null, "Catalog: Bad Type!");
                    }
                    break;

                default: {
                    throw new Catalogbadtype(null, "Catalog: Bad Type!");
                }
            }
        }


// CREATE TUPLE  

        tuple = new Tuple(Tuple.max_size);

        count = ExtendedSystemDefs.MINIBASE_ATTRCAT.getTupleStructure(relation,
                count, typeArray, sizeArray);

        tuple.setHdr((short) count, typeArray, sizeArray);


// CONVERT DATA STRINGS TO VARIABLE VALUES & INSERT INTO TUPLE

        for (int i = 0; i < relRec.attrCnt; i++) {
            switch (attrRecs[i].attrType.attrType) {

                case (AttrType.attrInteger):
                    Integer integerVal = new Integer(attrList[i].attrValue);
                    intVal = integerVal.intValue();
                    tuple.setIntFld(attrRecs[i].attrPos, intVal);
                    break;

                case (AttrType.attrReal):
                    Float floatVal1 = new Float(attrList[i].attrValue);
                    float fVal = floatVal1.floatValue();
                    tuple.setFloFld(attrRecs[i].attrPos, fVal);
                    break;

                case (AttrType.attrString):
                    tuple.setStrFld(attrRecs[i].attrPos, attrList[i].attrValue);
                    break;

                default:
                    System.out.println("Error in insertRecUT in utility.C");
            }
        }

        recSize = tuple.size();

// GET DATAFILE

        heap = new Heapfile(relation);


// INSERT INTO DATAFILE
        heap.insertRecord(tuple.getTupleByteArray());

// NOW INSERT INTO EACH INDEX FOR RELATION

        for (int i = 0; i < relRec.indexCnt; i++) {
            indexName = ExtendedSystemDefs.MINIBASE_INDCAT.buildIndexName(relation, indexRecs[i].attrName, //
                    indexRecs[i].accessType);

            // FIND INDEXED ATTRIBUTE

            for (int x = 0; x < attrCnt; x++) {
                if (attrRecs[x].attrName.equalsIgnoreCase(indexRecs[i].attrName) == true) {
                    attrType = attrRecs[x].attrType;
                    attrPos = attrRecs[x].attrPos;
                    break;
                }
            }

            // PULL OUT KEY

            switch (attrType.attrType) {
                case AttrType.attrInteger:
                    intVal = tuple.getIntFld(attrPos);
                    IntegerKey k1 = new IntegerKey(intVal);
                    key = k1;
                    break;

                case AttrType.attrReal:
                    floatVal = tuple.getFloFld(attrPos);
                    IntegerKey k2 = new IntegerKey((int) floatVal); // no FloatKey
                    key = k2;
                    break;

                case AttrType.attrString:
                    strVal = tuple.getStrFld(attrPos);
                    StringKey k3 = new StringKey(strVal);
                    key = k3;
                    break;
                default:
                    System.out.println("Error in insertRecUT");
            }


            // INSERT INTO INDEX

            // BTREE INSERT CODE

            if (indexRecs[i].accessType.indexType == IndexType.B_Index) {
                try {
                    btree = new BTreeFile(indexName);
                    if (btree == null)
                        throw new Catalognomem(null, "Catalog: No Enough Memory!");
                    btree.insert(key, rid);
                } catch (Exception e1) {
                    throw e1;
                }
            }

        } // end for loop - errors break out of loop
    }

    ;

    // WRAPS LOAD UTILITY IN TX
    void loadUT(String relation, String fileName) {
    }

    ;

    // LOADS RECORDS
    void loadRecordsUT(String relation, String fileName) {
    }

    ;

    // LOADS INDEXES
    void loadIndexesUT(Tuple tuple, int attrCnt, int indexCnt,
                       AttrDesc[] attrs, IndexDesc[] indexes, void[] iFiles, RID rid) {
    }

    ;

//-------------------------------
// TYPECHECK INTS
//--------------------------------

    /*Checks to see if a given string is a valid int.  ["-" | ""][0..9]+  */
    public static boolean check_int(attrNode N) {
        byte[] index;
        index = N.attrValue.getBytes();
        int length = N.attrValue.length();

        int count = 0;
        if ((length > 1) && (index[count] == '-'))
            count++;
        else
            return false;

        for (int i = count; i < length; i++)
            if ((index[i] < '0') || (index[i] > '9'))
                return false;

        return true;
    }


//-----------------------------------
//  TYPECHECK FLOAT
//------------------------------------

    /* CHecks to see if a string is a valid float.
    Nothing special.
    ["-" | ""] [0..9]+ ["." | ""] [0..9]+       */
    static boolean check_float(attrNode N) {
        byte[] index = N.attrValue.getBytes();
        int length = N.attrValue.length();

        int count = 0;
        if ((length > 1) && (index[count] == '-'))
            count++;
        else
            return false;

        if ((length > 1) && (index[count] == '.')) {   // If we begin with a ., then we must check to make
            // sure that all characters following it are numbers
            count++;
            for (int i = count; i < length; i++)
                if ((index[i] < '0') || (index[i] > '9'))
                    return false;
            return true;
        }

        // If the first character is NOT a number (ignoring minus signs),
        // then we must check fror numbers, then check for ., then check for numbers
        for (int i = count; i < length; i++) {
            if ((index[i] != '.') && ((index[i] < '0') || (index[i] > '9')))
                return false;

            // We've hit a ., so we must check for only numbers now..XS
            if (index[i] == '.')
                for (int j = i + 1; j < length; j++)
                    if ((index[j] < '0') || (index[j] > '9'))
                        return false;
        }
        return true;
    }

//-------------------------------------------------------------------
// CHECK STRING LENGTH
// Checks to make sure that the length of the string is within the liMits
// set by the attrDesc
//--------------------------------------------------------------------

    static boolean check_string(attrNode N) {
        return true;
    }


}
