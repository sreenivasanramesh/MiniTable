package BigT;

import cmdline.MiniTable;
import global.TupleOrder;
import heap.Heapfile;
import iterator.*;

public class RowSort {

    private String column;
    private Stream mapStream;
    private bigT bigTable;
    private Heapfile heapfile;
    private MapSort sortObj;

    public RowSort(String column, String outTable) throws Exception {
        this.column = column;
        this.bigTable = new bigT(outTable, true);
        this.heapfile = new Heapfile("temp_sort_file");
        insertTempHeapFile();
        createMapStream();

    }


    private void insertTempHeapFile() throws Exception {

        Stream tempStream = this.bigTable.openStream(1, "*", "*", "*");
        Map map = tempStream.getNext();
        String value = "";
        String row = "";

        while(map != null)
        {
            if(!map.getRowLabel().equals(row)){
                if(value.isEmpty()){
                    value = "zzzzz";
                }
                Map tempMap = new Map();
                tempMap.setRowLabel(row);
                tempMap.setColumnLabel("temp_column");
                tempMap.setValue(value);
                tempMap.setTimeStamp(1);
                this.heapfile.insertMap(tempMap.getMapByteArray());
                row = map.getRowLabel(); //next
                value = "";
            }
            if(map.getColumnLabel().equals(this.column)){
                value = map.getValue();
            }
            map = tempStream.getNext();
        }

        tempStream.closeStream();

        Map tempMap = new Map();
        tempMap.setRowLabel(row);
        tempMap.setColumnLabel("temp_column");
        tempMap.setValue(value);
        tempMap.setTimeStamp(1);
        this.heapfile.insertMap(tempMap.getMapByteArray());
    }

    private void createMapStream() throws Exception {
        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        FileScan fscan = null;
        try {
            fscan = new FileScan("temp_sort_file", MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            int num_pages = 10;
            this.sortObj = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, fscan, 4, new TupleOrder(TupleOrder.Ascending), num_pages, MiniTable.BIGT_STR_SIZES[1], false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map map = sortObj.get_next();
        this.mapStream = this.bigTable.openStream(9999, map.getRowLabel(), "*", "*");

    }

    public Map getNext() throws Exception {
        Map map = this.mapStream.getNext();
        if(map == null){
            mapStream.closeStream();
            Map NextVal = this.sortObj.get_next();
            if (NextVal == null)
                return null;
            Stream tempMapStream = this.bigTable.openStream(9999, NextVal.getRowLabel(), "*", "*");
            map = tempMapStream.getNext();
            if(map == null)
                tempMapStream.closeStream();
        }

        return map;
    }

    public void closeStream() throws Exception{
        this.sortObj.close();
        heapfile.deleteFile();
    }



}
