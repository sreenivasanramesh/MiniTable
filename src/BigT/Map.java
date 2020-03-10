package BigT;

import global.AttrType;
import global.Convert;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidMapSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

public class Map implements GlobalConst {

    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    private static final short OFFSET_INCREMENT = 4;
    private static final short FIELD_LENGTH = 4;
    private static final short ROW_POSITION = 0;
    private static final short COLUMN_POSITION = 4;
    private static final short TIMESTAMP_POSITION = 8;
    private static final short VALUE_POSITION = 12;
    private byte[] data;
    private int mapOffset;
    private int mapLength;
    private short fieldCount;
    private short[] fieldOffset;

    public Map() {
        this.data = new byte[MAX_SIZE];
        this.mapOffset = 0;
        this.mapLength = MAX_SIZE;
    }

    public Map(byte[] amap, int offset) {
        this.data = amap;
        this.mapOffset = offset;
    }

    public Map(byte[] amap, int offset, int mapLength) {
        this.data = amap;
        this.mapOffset = offset;
        this.mapLength = mapLength;
    }

    public Map(Map fromMap) {
        this.data = fromMap.getMapByteArray();
        this.mapLength = fromMap.getMapLength();
        this.mapOffset = 0;
        this.fieldCount = fromMap.getFieldCount();
        this.fieldOffset = fromMap.copyFieldOffset();

    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
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
            return Convert.getStrValue(this.mapOffset + fieldNumber * FIELD_LENGTH, this.data, FIELD_LENGTH);
        }
    }

    public short[] copyFieldOffset() {
        short[] newFieldOffset = new short[this.fieldCount + 1];
        System.arraycopy(this.fieldOffset, 0, newFieldOffset, 0, this.fieldCount + 1);
        return newFieldOffset;
    }

    public void copyMap(Map fromMap) {
        byte[] tempArray = fromMap.getMapByteArray();
        System.arraycopy(tempArray, 0, data, mapOffset, mapLength);
    }

    public String getRowLabel() throws IOException {
        return Convert.getStrValue(this.mapOffset + ROW_POSITION, this.data, FIELD_LENGTH);
    }

    public void setRowLabel(String rowLabel) throws IOException {
        Convert.setStrValue(rowLabel, this.mapOffset + ROW_POSITION, this.data);
    }

    public String getColumnLabel() throws IOException {
        return Convert.getStrValue(this.mapOffset + COLUMN_POSITION, this.data, FIELD_LENGTH);
    }

    public void setColumnLabel(String columnLabel) throws IOException {
        Convert.setStrValue(columnLabel, this.mapOffset + COLUMN_POSITION, this.data);
    }

    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(this.mapOffset + TIMESTAMP_POSITION, this.data);
    }

    public void setTimeStamp(int timeStamp) throws IOException {
        Convert.setIntValue(timeStamp, this.mapOffset + TIMESTAMP_POSITION, this.data);
    }

    public String getValue() throws IOException {
        return Convert.getStrValue(this.mapOffset + VALUE_POSITION, this.data, FIELD_LENGTH);
    }

    public void setValue(String value) throws IOException {
        Convert.setStrValue(value, this.mapOffset + VALUE_POSITION, this.data);
    }

    public byte[] getMapByteArray() {
        byte[] mapCopy = new byte[this.mapLength];
        System.arraycopy(this.data, this.mapOffset, mapCopy, 0, mapLength);
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

    // TODO: This method needs to be altered to set a proper header
    public void setHeader(short numFields, AttrType[] types) throws InvalidMapSizeException, IOException, InvalidTypeException {


        if ((numFields + 2) * 2 > MAX_SIZE) {
            throw new InvalidMapSizeException(null, "MAP: MAP TOO BIG ERROR");
        }
        this.fieldCount = numFields;
        Convert.setShortValue(numFields, this.mapOffset, this.data);
        this.fieldOffset = new short[numFields + 1];
        int position = this.mapOffset + 2;
        this.fieldOffset[0] = (short) ((numFields + 2) * 2 + this.mapOffset);
        Convert.setShortValue(this.fieldOffset[0], position, data);
        position += 2;

        for (short i = 0; i < numFields; i++) {
            switch (types[i].attrType) {
                case AttrType.attrInteger:
                case AttrType.attrString:
                    break;

                default:
                    throw new InvalidTypeException(null, "MAP: MAP_TYPE_ERROR");
            }
            this.fieldOffset[i + 1] = (short) (this.fieldOffset[i] + OFFSET_INCREMENT);
            Convert.setShortValue(this.fieldOffset[i + 1], position, data);
            position += 2;
        }

        this.mapLength = this.fieldOffset[numFields] - this.mapOffset;

        if (this.mapLength > MAX_SIZE) {
            throw new InvalidMapSizeException(null, "MAP: MAP_TOOBIG_ERROR");
        }

    }

}