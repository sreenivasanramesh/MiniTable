package BigT;

import global.AttrType;
import global.Convert;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidMapSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

public class Map implements GlobalConst {

    public static final short NUM_FIELDS = 4;
    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    private static final short ROW_NUMBER = 1;
    private static final short COLUMN_NUMBER = 2;
    private static final short TIMESTAMP_NUMBER = 3;
    private static final short VALUE_NUMBER = 4;
    private byte[] data;
    private int mapOffset;
    private int mapLength;
    private short fieldCount;
    private short[] fieldOffset;

    /**
     * Default Map constructor to initialise a new map.
     */
    public Map() {
        this.data = new byte[MAX_SIZE];
        this.mapOffset = 0;
        this.mapLength = MAX_SIZE;
    }

    /**
     * @param amap Initialise map based on bytearray from given map.
     * @param offset map offset.
     * @throws IOException throws IO exception
     */
    public Map(byte[] amap, int offset) throws IOException {
        this.data = amap;
        this.mapOffset = offset;
        setFieldOffsetFromData();
        setFieldCount(Convert.getShortValue(offset, this.data));
    }

    /**
     * @param amap Initialise map based on bytearray from given map.
     * @param offset map offset.
     * @param mapLength Set length of the map.
     * @throws IOException throws IO Exception.
     */
    public Map(byte[] amap, int offset, int mapLength) throws IOException {
        this.data = amap;
        this.mapOffset = offset;
        this.mapLength = mapLength;
        setFieldOffsetFromData();
        setFieldCount(Convert.getShortValue(offset, this.data));
    }

    /**
     * @param fromMap Initialse maps given map object.
     */
    public Map(Map fromMap) {
        this.data = fromMap.getMapByteArray();
        this.mapLength = fromMap.getMapLength();
        this.mapOffset = 0;
        this.fieldCount = fromMap.getFieldCount();
        this.fieldOffset = fromMap.copyFieldOffset();

    }

    /**
     * @param size Initialze map based on given size.
     */
    public Map(int size) {
        this.data = new byte[size];
        this.mapOffset = 0;
        this.mapLength = size;
        this.fieldCount = 4;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) throws IOException {
        this.data = data;
        setFieldOffsetFromData();
        setFieldCount(Convert.getShortValue(0, data));
    }

    public int getMapOffset() {
        return mapOffset;
    }

    public void setMapOffset(int mapOffset) {
        this.mapOffset = mapOffset;
    }

    public int getMapLength() {
        return mapLength;
    }

    public void setMapLength(int mapLength) {
        this.mapLength = mapLength;
    }

    public short getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(short fieldCount) {
        this.fieldCount = fieldCount;
    }

    public short[] getFieldOffset() {
        return fieldOffset;
    }

    public void setFieldOffset(short[] fieldOffset) {
        this.fieldOffset = fieldOffset;
    }

    public String getStringField(short fieldNumber) throws IOException, FieldNumberOutOfBoundException {
        if (fieldNumber == 3) {
            throw new FieldNumberOutOfBoundException(null, "MAP: INVALID_FIELD PASSED");
        } else {
            return Convert.getStrValue(this.fieldOffset[fieldNumber - 1], this.data, this.fieldOffset[fieldNumber] - this.fieldOffset[fieldNumber - 1]);
        }
    }

    public short[] copyFieldOffset() {
        short[] newFieldOffset = new short[this.fieldCount + 1];
        System.arraycopy(this.fieldOffset, 0, newFieldOffset, 0, this.fieldCount + 1);
        return newFieldOffset;
    }

    /**
     * @param fromMap Copy the map object to this map object.
     */
    public void copyMap(Map fromMap) {
        byte[] tempArray = fromMap.getMapByteArray();
        System.arraycopy(tempArray, 0, data, mapOffset, mapLength);
    }

    public String getRowLabel() throws IOException {
        return Convert.getStrValue(this.fieldOffset[ROW_NUMBER - 1], this.data, this.fieldOffset[ROW_NUMBER] - this.fieldOffset[ROW_NUMBER - 1]);
    }

    public void setRowLabel(String rowLabel) throws IOException {
        Convert.setStrValue(rowLabel, this.fieldOffset[ROW_NUMBER - 1], this.data);
    }

    public String getColumnLabel() throws IOException {
        return Convert.getStrValue(this.fieldOffset[COLUMN_NUMBER - 1], this.data, this.fieldOffset[COLUMN_NUMBER] - this.fieldOffset[COLUMN_NUMBER - 1]);
    }

    public void setColumnLabel(String columnLabel) throws IOException {
        Convert.setStrValue(columnLabel, this.fieldOffset[COLUMN_NUMBER - 1], this.data);
    }

    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(this.fieldOffset[TIMESTAMP_NUMBER - 1], this.data);
    }

    public void setTimeStamp(int timeStamp) throws IOException {
        Convert.setIntValue(timeStamp, this.fieldOffset[TIMESTAMP_NUMBER - 1], this.data);
    }

    public String getValue() throws IOException {
        return Convert.getStrValue(this.fieldOffset[VALUE_NUMBER - 1], this.data, this.fieldOffset[VALUE_NUMBER] - this.fieldOffset[VALUE_NUMBER - 1]);
    }

    public void setValue(String value) throws IOException {
        Convert.setStrValue(value, this.fieldOffset[VALUE_NUMBER - 1], this.data);
    }

    public byte[] getMapByteArray() {
        byte[] mapCopy = new byte[this.mapLength];
        System.arraycopy(this.data, this.mapOffset, mapCopy, 0, this.mapLength);
        return mapCopy;
    }

    public void print() throws IOException {
        String rowLabel = getRowLabel();
        String columnLabel = getColumnLabel();
        int timestamp = getTimeStamp();
        String value = getValue();
        System.out.println("{RowLabel:" + rowLabel + ", ColumnLabel:" + columnLabel + ", TimeStamp:" + timestamp + ", Value:" + value + "}");
    }

    public short size() {
        return ((short) (this.fieldOffset[fieldCount] - this.mapOffset));
    }

    public void mapInit(byte[] amap, int offset) {
        this.data = amap;
        this.mapOffset = offset;
    }

    public void mapSet(byte[] fromMap, int offset) {
        System.arraycopy(fromMap, offset, this.data, 0, this.mapLength);
        this.mapOffset = 0;
    }
    
    private void setFieldOffsetFromData() throws IOException {
        int position = this.mapOffset + 2;
        this.fieldOffset = new short[NUM_FIELDS + 1];
        
        for (int i = 0; i <= NUM_FIELDS; i++) {
            this.fieldOffset[i] = Convert.getShortValue(position, this.data);
            position += 2;
        }
    }
    
    @Override
    public String toString() {
        String rowLabel = null;
        String columnLabel = null;
        String value = null;
        int timestamp = 0;
        try {
            rowLabel = getRowLabel();
            columnLabel = getColumnLabel();
            timestamp = getTimeStamp();
            value = getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String s = new String("{RowLabel:" + rowLabel + ", ColumnLabel:" + columnLabel + ", TimeStamp:" + timestamp + ", Value:" + value + "}");
        return s;
    }
    
    public void setHeader(AttrType[] types, short[] stringSizes) throws InvalidMapSizeException, IOException, InvalidTypeException, InvalidStringSizeArrayException {
        
        if (stringSizes.length != 3) {
            throw new InvalidStringSizeArrayException(null, "String sizes array must exactly be 3");
        }
        this.fieldCount = NUM_FIELDS;
        Convert.setShortValue(NUM_FIELDS, this.mapOffset, this.data);
        this.fieldOffset = new short[NUM_FIELDS + 1];
        int position = this.mapOffset + 2;
        this.fieldOffset[0] = (short) ((NUM_FIELDS + 2) * 2 + this.mapOffset);
        Convert.setShortValue(this.fieldOffset[0], position, data);
        position += 2;

        short increment;
        short stringCount = 0;
        for (short i = 0; i < NUM_FIELDS; i++) {
            switch (types[i].attrType) {
                case AttrType.attrInteger:
                    increment = 4;
                    break;
                case AttrType.attrString:
                    increment = (short) (stringSizes[stringCount++] + 2);
                    break;
                default:
                    throw new InvalidTypeException(null, "MAP: MAP_TYPE_ERROR");
            }
            this.fieldOffset[i + 1] = (short) (this.fieldOffset[i] + increment);
            Convert.setShortValue(this.fieldOffset[i + 1], position, data);
            position += 2;
        }

        this.mapLength = this.fieldOffset[NUM_FIELDS] - this.mapOffset;

        if (this.mapLength > MAX_SIZE) {
            throw new InvalidMapSizeException(null, "MAP: MAP_TOOBIG_ERROR");
        }

    }

    public String getGenericValue(String field) throws Exception {
        if (field.matches(".*row.*")) {
            return this.getRowLabel();
        } else if (field.matches(".*column.*")) {
            return this.getColumnLabel();
        } else if (field.matches(".*value.*")) {
            return this.getValue();
        } else {
            throw new Exception("Invalid field type.");
        }
    }

    public Map setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setStrValue(val, fieldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setIntValue(val, fieldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public Map setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setFloValue(val, fieldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");

    }

    public int getIntFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
        int val;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            val = Convert.getIntValue(fieldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public float getFloFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        float val;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            val = Convert.getFloValue(fieldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    public String getStrFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        String val;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            val = Convert.getStrValue(fieldOffset[fldNo - 1], data,
                    fieldOffset[fldNo] - fieldOffset[fldNo - 1]); //strlen+2
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }
//    public String getStringField(short fieldNumber) throws IOException, FieldNumberOutOfBoundException {
//        if (fieldNumber == 3) {
//            throw new FieldNumberOutOfBoundException(null, "MAP: INVALID_FIELD PASSED");
//        } else {
//            return Convert.getStrValue(this.fieldOffset[fieldNumber - 1], this.data, this.fieldOffset[fieldNumber] - this.fieldOffset[fieldNumber - 1]);
//        }
//    }


}
