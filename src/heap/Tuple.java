/* File Tuple.java */

package heap;

import java.io.*;
import java.lang.*;

import global.*;


public class Tuple implements GlobalConst {


    /**
     * Maximum size of any tuple
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte[] data;

    /**
     * start position of this tuple in data[]
     */
    private int tuple_offset;

    /**
     * length of this tuple
     */
    private int tuple_length;

    /**
     * private field
     * Number of fields in this tuple
     */
    private short fldCnt;

    /**
     * private field
     * Array of offsets of the fields
     */

    private short[] fldOffset;

    /**
     * Class constructor
     * Creat a new tuple with length = max_size,tuple offset = 0.
     */

    public Tuple() {
        // Creat a new tuple
        data = new byte[max_size];
        tuple_offset = 0;
        tuple_length = max_size;
    }

    /**
     * Constructor
     *
     * @param atuple a byte array which contains the tuple
     * @param offset the offset of the tuple in the byte array
     * @param length the length of the tuple
     */

    public Tuple(byte[] atuple, int offset, int length) {
        data = atuple;
        tuple_offset = offset;
        tuple_length = length;
        //  fldCnt = getShortValue(offset, data);
    }

    /**
     * Constructor(used as tuple copy)
     *
     * @param fromTuple a byte array which contains the tuple
     */
    public Tuple(Tuple fromTuple) {
        data = fromTuple.getTupleByteArray();
        tuple_length = fromTuple.getLength();
        tuple_offset = 0;
        fldCnt = fromTuple.noOfFlds();
        fldOffset = fromTuple.copyFldOffset();
    }

    /**
     * Class constructor
     * Creat a new tuple with length = size,tuple offset = 0.
     */

    public Tuple(int size) {
        // Creat a new tuple
        data = new byte[size];
        tuple_offset = 0;
        tuple_length = size;
    }

    /**
     * Copy a tuple to the current tuple position
     * you must make sure the tuple lengths must be equal
     *
     * @param fromTuple the tuple being copied
     */
    public void tupleCopy(Tuple fromTuple) {
        byte[] temparray = fromTuple.getTupleByteArray();
        System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);
//       fldCnt = fromTuple.noOfFlds(); 
//       fldOffset = fromTuple.copyFldOffset(); 
    }

    /**
     * This is used when you don't want to use the constructor
     *
     * @param atuple a byte array which contains the tuple
     * @param offset the offset of the tuple in the byte array
     * @param length the length of the tuple
     */

    public void tupleInit(byte[] atuple, int offset, int length) {
        data = atuple;
        tuple_offset = offset;
        tuple_length = length;
    }

    /**
     * Set a tuple with the given tuple length and offset
     *
     * @param    record    a byte array contains the tuple
     * @param    offset the offset of the tuple ( =0 by default)
     * @param    length    the length of the tuple
     */
    public void tupleSet(byte[] record, int offset, int length) {
        System.arraycopy(record, offset, data, 0, length);
        tuple_offset = 0;
        tuple_length = length;
    }

    /**
     * get the length of a tuple, call this method if you did not
     * call setHdr () before
     *
     * @return length of this tuple in bytes
     */
    public int getLength() {
        return tuple_length;
    }

    /**
     * get the length of a tuple, call this method if you did
     * call setHdr () before
     *
     * @return size of this tuple in bytes
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - tuple_offset));
    }

    /**
     * get the offset of a tuple
     *
     * @return offset of the tuple in byte array
     */
    public int getOffset() {
        return tuple_offset;
    }

    /**
     * Copy the tuple byte array out
     *
     * @return byte[], a byte array contains the tuple
     * the length of byte[] = length of the tuple
     */

    public byte[] getTupleByteArray() {
        byte[] tuplecopy = new byte[tuple_length];
        System.arraycopy(data, tuple_offset, tuplecopy, 0, tuple_length);
        return tuplecopy;
    }

    /**
     * return the data byte array
     *
     * @return data byte array
     */

    public byte[] returnTupleByteArray() {
        return data;
    }

    /**
     * Convert this field into integer
     *
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
     * @param    fldNo    the field number
     * @return the converted integer if success
     */

    public int getIntFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        int val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getIntValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field in to float
     *
     * @param fldNo the field number
     * @return the converted float number  if success
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public float getFloFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        float val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getFloValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }


    /**
     * Convert this field into String
     *
     * @param fldNo the field number
     * @return the converted string if success
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public String getStrFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        String val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getStrValue(fldOffset[fldNo - 1], data,
                    fldOffset[fldNo] - fldOffset[fldNo - 1]); //strlen+2
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field into a character
     *
     * @param fldNo the field number
     * @return the character if success
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public char getCharFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        char val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getCharValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to integer value
     *
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
     * @param    fldNo    the field number
     * @param    val    the integer value
     */

    public Tuple setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setIntValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Set this field to float value
     *
     * @param fldNo the field number
     * @param val   the float value
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public Tuple setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setFloValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to String value
     *
     * @param fldNo the field number
     * @param val   the string value
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public Tuple setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setStrValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }


    /**
     * setHdr will set the header of this tuple.
     *
     * @throws IOException               I/O errors
     * @throws InvalidTypeException      Invalid tupe type
     * @throws InvalidTupleSizeException Tuple size too big
     * @param    numFlds     number of fields
     * @param    types[]     contains the types that will be in this tuple
     * @param    strSizes[] contains the sizes of the string
     */

    public void setHdr(short numFlds, AttrType types[], short strSizes[])
            throws IOException, InvalidTypeException, InvalidTupleSizeException {
        if ((numFlds + 2) * 2 > max_size)
            throw new InvalidTupleSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");

        fldCnt = numFlds;
        Convert.setShortValue(numFlds, tuple_offset, data);
        fldOffset = new short[numFlds + 1];
        int pos = tuple_offset + 2;  // start position for fldOffset[]

        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
        //another 1 for fldCnt
        fldOffset[0] = (short) ((numFlds + 2) * 2 + tuple_offset);

        Convert.setShortValue(fldOffset[0], pos, data);
        pos += 2;
        short strCount = 0;
        short incr;
        int i;

        for (i = 1; i < numFlds; i++) {
            switch (types[i - 1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrReal:
                    incr = 4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidTypeException(null, "TUPLE: TUPLE_TYPE_ERROR");
            }
            fldOffset[i] = (short) (fldOffset[i - 1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos += 2;

        }
        switch (types[numFlds - 1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrReal:
                incr = 4;
                break;

            case AttrType.attrString:
                incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidTypeException(null, "TUPLE: TUPLE_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, data);

        tuple_length = fldOffset[numFlds] - tuple_offset;

        if (tuple_length > max_size)
            throw new InvalidTupleSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");
    }


    /**
     * Returns number of fields in this tuple
     *
     * @return the number of fields in this tuple
     */

    public short noOfFlds() {
        return fldCnt;
    }

    /**
     * Makes a copy of the fldOffset array
     *
     * @return a copy of the fldOffset arrray
     */

    public short[] copyFldOffset() {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i = 0; i <= fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }

    /**
     * Print out the tuple
     *
     * @param type the types in the tuple
     * @Exception IOException I/O exception
     */
    public void print(AttrType type[])
            throws IOException {
        int i, val;
        float fval;
        String sval;

        System.out.print("[");
        for (i = 0; i < fldCnt - 1; i++) {
            switch (type[i].attrType) {

                case AttrType.attrInteger:
                    val = Convert.getIntValue(fldOffset[i], data);
                    System.out.print(val);
                    break;

                case AttrType.attrReal:
                    fval = Convert.getFloValue(fldOffset[i], data);
                    System.out.print(fval);
                    break;

                case AttrType.attrString:
                    sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
                    System.out.print(sval);
                    break;

                case AttrType.attrNull:
                case AttrType.attrSymbol:
                    break;
            }
            System.out.print(", ");
        }

        switch (type[fldCnt - 1].attrType) {

            case AttrType.attrInteger:
                val = Convert.getIntValue(fldOffset[i], data);
                System.out.print(val);
                break;

            case AttrType.attrReal:
                fval = Convert.getFloValue(fldOffset[i], data);
                System.out.print(fval);
                break;

            case AttrType.attrString:
                sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
                System.out.print(sval);
                break;

            case AttrType.attrNull:
            case AttrType.attrSymbol:
                break;
        }
        System.out.println("]");

    }

    /**
     * private method
     * Padding must be used when storing different types.
     *
     * @param type the type of tuple
     * @return short typle
     * @param    offset
     */

    private short pad(short offset, AttrType type) {
        return 0;
    }
}

