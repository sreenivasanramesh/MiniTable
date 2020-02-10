//------------------------------------
// AttrCatalog.java
//
// Ning Wang, April,24,  1998
//-------------------------------------

package catalog;

import java.io.*;

import global.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;


public class AttrCatalog extends Heapfile
        implements GlobalConst, Catalogglobal {
    //OPEN ATTRIBUTE CATALOG
    AttrCatalog(String filename)
            throws java.io.IOException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            AttrCatalogException {
        super(filename);

        int sizeOfInt = 4;
        int sizeOfFloat = 4;
        tuple = new Tuple(Tuple.max_size);
        attrs = new AttrType[9];

        attrs[0] = new AttrType(AttrType.attrString);
        attrs[1] = new AttrType(AttrType.attrString);
        attrs[2] = new AttrType(AttrType.attrInteger);
        attrs[3] = new AttrType(AttrType.attrInteger);
        attrs[4] = new AttrType(AttrType.attrInteger);
        // AttrType will be represented by an integer
        // 0 = string, 1 = real, 2 = integer
        attrs[5] = new AttrType(AttrType.attrInteger);
        attrs[6] = new AttrType(AttrType.attrInteger);
        attrs[7] = new AttrType(AttrType.attrString);   // ?????  BK ?????
        attrs[8] = new AttrType(AttrType.attrString);   // ?????  BK ?????


        // Find the largest possible tuple for values attrs[7] & attrs[8]
        //   str_sizes[2] & str_sizes[3]
        max = 10;   // comes from attrData char strVal[10]
        if (sizeOfInt > max)
            max = (short) sizeOfInt;
        if (sizeOfFloat > max)
            max = (short) sizeOfFloat;


        str_sizes = new short[4];
        str_sizes[0] = (short) MAXNAME;
        str_sizes[1] = (short) MAXNAME;
        str_sizes[2] = max;
        str_sizes[3] = max;

        try {
            tuple.setHdr((short) 9, attrs, str_sizes);
        } catch (Exception e) {
            throw new AttrCatalogException(e, "setHdr() failed");
        }
    }

    ;

    // GET ATTRIBUTE DESCRIPTION
    public void getInfo(String relation, String attrName, AttrDesc record)
            throws Catalogmissparam,
            Catalogioerror,
            Cataloghferror,
            AttrCatalogException,
            IOException,
            Catalogattrnotfound {
        int recSize;
        RID rid = null;
        Scan pscan = null;


        if ((relation == null) || (attrName == null))
            throw new Catalogmissparam(null, "MISSING_PARAM");

        // OPEN SCAN

        try {
            pscan = new Scan(this);
        } catch (Exception e1) {
            throw new AttrCatalogException(e1, "scan failed");
        }

        // SCAN FILE FOR ATTRIBUTE
        // NOTE MUST RETURN ATTRNOTFOUND IF NOT FOUND!!!

        while (true) {
            try {
                tuple = pscan.getNext(rid);
                if (tuple == null)
                    throw new Catalogattrnotfound(null, "Catalog: Attribute not Found!");
                read_tuple(tuple, record);
            } catch (Exception e4) {
                throw new AttrCatalogException(e4, "read_tuple failed");
            }

            if (record.relName.equalsIgnoreCase(relation) == true
                    && record.attrName.equalsIgnoreCase(attrName) == true)
                return;
        }
    }

    ;

    // GET ALL ATTRIBUTES OF A RELATION/
    // Return attrCnt
    public int getRelInfo(String relation, int attrCnt, AttrDesc[] Attrs)
            throws Catalogmissparam,
            Catalogioerror,
            Cataloghferror,
            AttrCatalogException,
            IOException,
            Catalognomem,
            Catalogattrnotfound,
            Catalogindexnotfound,
            Catalogrelnotfound {
        RelDesc record = null;
        AttrDesc attrRec = null;
        int status;
        int recSize;
        RID rid = null;
        Scan pscan = null;
        int count = 0;

        if (relation == null)
            throw new Catalogmissparam(null, "MISSING_PARAM");

        try {
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relation, record);
        } catch (Catalogioerror e) {
            System.err.println("Catalog I/O Error!" + e);
            throw new Catalogioerror(null, "");
        } catch (Cataloghferror e1) {
            System.err.println("Catalog Heapfile Error!" + e1);
            throw new Cataloghferror(null, "");
        } catch (Catalogmissparam e2) {
            System.err.println("Catalog Missing Param Error!" + e2);
            throw new Catalogmissparam(null, "");
        } catch (Catalogrelnotfound e3) {
            System.err.println("Catalog: Relation not Found!" + e3);
            throw new Catalogrelnotfound(null, "");
        } catch (Exception e4) {
            e4.printStackTrace();
            throw new AttrCatalogException(e4, "getInfo() failed");
        }

        // SET ATTRIBUTE COUNT BY REFERENCE
        attrCnt = record.attrCnt;
        if (attrCnt == 0)
            return attrCnt;


        // OPEN SCAN

        try {
            pscan = new Scan(this);
        } catch (Exception e1) {
            throw new AttrCatalogException(e1, "scan failed");
        }

        // ALLOCATE ARRAY

        Attrs = new AttrDesc[attrCnt];
        if (Attrs == null)
            throw new Catalognomem(null, "Catalog: No Enough Memory!");

        // SCAN FILE

        while (true) {
            try {
                tuple = pscan.getNext(rid);
                if (tuple == null)
                    throw new Catalogindexnotfound(null,
                            "Catalog: Index not Found!");
                read_tuple(tuple, attrRec);
            } catch (Exception e4) {
                throw new AttrCatalogException(e4, "read_tuple failed");
            }

            if (attrRec.relName.equalsIgnoreCase(relation) == true) {
                Attrs[attrRec.attrPos - 1] = attrRec;
                count++;
            }

            if (count == attrCnt)  // if all atts found
                break;
        }
        return attrCnt;
    }

    ;

    // RETURNS ATTRTYPE AND STRINGSIZE ARRAYS FOR CONSTRUCTING TUPLES
    public int getTupleStructure(String relation, int attrCnt,
                                 AttrType[] typeArray, short[] sizeArray)
            throws Catalogmissparam,
            Catalogioerror,
            Cataloghferror,
            AttrCatalogException,
            IOException,
            Catalognomem,
            Catalogindexnotfound,
            Catalogattrnotfound,
            Catalogrelnotfound {
        int status;
        int stringcount = 0;
        AttrDesc[] attrs = null;
        int i, x;

        // GET ALL OF THE ATTRIBUTES

        try {
            attrCnt = getRelInfo(relation, attrCnt, attrs);
        } catch (Catalogioerror e) {
            System.err.println("Catalog I/O Error!" + e);
            throw new Catalogioerror(null, "");
        } catch (Cataloghferror e1) {
            System.err.println("Catalog Heapfile Error!" + e1);
            throw new Cataloghferror(null, "");
        } catch (Catalogmissparam e2) {
            System.err.println("Catalog Missing Param Error!" + e2);
            throw new Catalogmissparam(null, "");
        } catch (Catalogindexnotfound e3) {
            System.err.println("Catalog Index not Found!" + e3);
            throw new Catalogindexnotfound(null, "");
        } catch (Catalogattrnotfound e4) {
            System.err.println("Catalog: Attribute not Found!" + e4);
            throw new Catalogattrnotfound(null, "");
        } catch (Catalogrelnotfound e5) {
            System.err.println("Catalog: Relation not Found!" + e5);
            throw new Catalogrelnotfound(null, "");
        }


        // ALLOCATE TYPEARRAY

        typeArray = new AttrType[attrCnt];
        if (typeArray == null)
            throw new Catalognomem(null, "Catalog, No Enough Memory!");

        // LOCATE STRINGS

        for (i = 0; i < attrCnt; i++) {
            if (attrs[i].attrType.attrType == AttrType.attrString)
                stringcount++;
        }

        // ALLOCATE STRING SIZE ARRAY

        if (stringcount > 0) {
            sizeArray = new short[stringcount];
            if (sizeArray == null)
                throw new Catalognomem(null, "Catalog, No Enough Memory!");
        }

        // FILL ARRAYS WITH TYPE AND SIZE DATA

        for (x = 0, i = 0; i < attrCnt; i++) {
            typeArray[i].attrType = attrs[i].attrType.attrType;
            if (attrs[i].attrType.attrType == AttrType.attrString) {
                sizeArray[x] = (short) attrs[i].attrLen;
                x++;
            }
        }

        return attrCnt;
    }

    ;


    // ADD ATTRIBUTE ENTRY TO CATALOG
    public void addInfo(AttrDesc record)
            throws AttrCatalogException,
            IOException {
        RID rid;

        try {
            make_tuple(tuple, record);
        } catch (Exception e4) {
            throw new AttrCatalogException(e4, "make_tuple failed");
        }

        try {
            insertRecord(tuple.getTupleByteArray());
        } catch (Exception e2) {
            throw new AttrCatalogException(e2, "insertRecord failed");
        }
    }

    ;


    // REMOVE AN ATTRIBUTE ENTRY FROM CATALOG
    // return true if success, false if not found.
    public void removeInfo(String relation, String attrName)
            throws AttrCatalogException,
            IOException,
            Catalogmissparam,
            Catalogattrnotfound {
        int recSize;
        RID rid = null;
        Scan pscan = null;
        AttrDesc record = null;


        if ((relation == null) || (attrName == null))
            throw new Catalogmissparam(null, "MISSING_PARAM");

        // OPEN SCAN
        try {
            pscan = new Scan(this);
        } catch (Exception e1) {
            throw new AttrCatalogException(e1, "scan failed");
        }


        // SCAN FILE
        while (true) {
            try {
                tuple = pscan.getNext(rid);
                if (tuple == null)
                    throw new Catalogattrnotfound(null,
                            "Catalog: Attribute not Found!");
                read_tuple(tuple, record);
            } catch (Exception e4) {
                throw new AttrCatalogException(e4, "read_tuple failed");
            }

            if (record.relName.equalsIgnoreCase(relation) == true
                    && record.attrName.equalsIgnoreCase(attrName) == true) {
                try {
                    deleteRecord(rid);
                } catch (Exception e3) {
                    throw new AttrCatalogException(e3, "deleteRecord failed");
                }
                return;
            }
        }
    }

    ;


    //--------------------------------------------------
    // MAKE_TUPLE
    //--------------------------------------------------
    // Tuple must have been initialized properly in the
    // constructor
    // Converts AttrDesc to tuple.
    public void make_tuple(Tuple tuple, AttrDesc record)
            throws IOException,
            AttrCatalogException {
        try {
            tuple.setStrFld(1, record.relName);
            tuple.setStrFld(2, record.attrName);
            tuple.setIntFld(3, record.attrOffset);
            tuple.setIntFld(4, record.attrPos);

            if (record.attrType.attrType == AttrType.attrString) {
                tuple.setIntFld(5, 0);
                tuple.setStrFld(8, record.minVal.strVal);
                tuple.setStrFld(9, record.maxVal.strVal);
            } else if (record.attrType.attrType == AttrType.attrReal) {
                tuple.setIntFld(5, 1);
                tuple.setFloFld(8, record.minVal.floatVal);
                tuple.setFloFld(9, record.minVal.floatVal);
            } else {
                tuple.setIntFld(5, 2);
                tuple.setIntFld(8, record.minVal.intVal);
                tuple.setIntFld(9, record.maxVal.intVal);
            }

            tuple.setIntFld(6, record.attrLen);
            tuple.setIntFld(7, record.indexCnt);
        } catch (Exception e1) {
            throw new AttrCatalogException(e1, "make_tuple failed");
        }
    }

    ;


    //--------------------------------------------------
    // READ_TUPLE
    //--------------------------------------------------

    public void read_tuple(Tuple tuple, AttrDesc record)
            throws IOException,
            AttrCatalogException {
        try {
            record.relName = tuple.getStrFld(1);
            record.attrName = tuple.getStrFld(2);
            record.attrOffset = tuple.getIntFld(3);
            record.attrPos = tuple.getIntFld(4);

            int temp;
            temp = tuple.getIntFld(5);
            if (temp == 0) {
                record.attrType = new AttrType(AttrType.attrString);
                record.minVal.strVal = tuple.getStrFld(8);
                record.maxVal.strVal = tuple.getStrFld(9);
            } else if (temp == 1) {
                record.attrType = new AttrType(AttrType.attrReal);
                record.minVal.floatVal = tuple.getFloFld(8);
                record.maxVal.floatVal = tuple.getFloFld(9);
            } else if (temp == 2) {
                record.attrType = new AttrType(AttrType.attrInteger);
                record.minVal.intVal = tuple.getIntFld(8);
                record.maxVal.intVal = tuple.getIntFld(9);
            } else {
                return;
            }

            record.attrLen = tuple.getIntFld(6);
            record.indexCnt = tuple.getIntFld(7);
        } catch (Exception e1) {
            throw new AttrCatalogException(e1, "read_tuple failed");
        }

    }

    // REMOVE ALL ATTRIBUTE ENTRIES FOR A RELATION
    public void dropRelation(String relation) {
    }

    ;

    // ADD AN INDEX TO A RELATION
    public void addIndex(String relation, String attrname,
                         IndexType accessType) {
    }

    ;


    Tuple tuple;
    short[] str_sizes;
    AttrType[] attrs;
    short max;
};
