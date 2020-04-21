package BigT;

import heap.Heapfile;
import iterator.Sort;

public class RowSort {

    private String inTable;
    private String outTable;
    private String column;
    private int numBuffers;
    private Stream mapStream;
    private Heapfile heapfile;
    private Sort sort;

    public RowSort(String inTable, String outTable, String column, int numBuffers) throws Exception {
        this.inTable = inTable;
        this.outTable = outTable;
        this.column = column;
        this.numBuffers = numBuffers;

        bigT bigTable = new bigT(this.outTable, true);
        this.mapStream = bigTable.openStream(1, "*", "*", "*");
        this.heapfile = new Heapfile("temp_sort_file");

        Map map = mapStream.getNext();




    }

    public void sort() throws Exception {


        Map map = mapStream.getNext();
        String value;
        String row = null;


        while(map != null){

            if(map.getColumnLabel().equalsIgnoreCase(this.column)){
                value = map.getValue();
            }
            if(!map.getRowLabel().equalsIgnoreCase(row)){
                if(value.isEmpty()){
                    value = "zzzzz";
                }
                Map tempMap = new Map();
                map.se

                this.heapfile.insertMap(tempMap.getMapByteArray());

            }

            //do something

        }
        map = mapStream.getNext();
        Map tempMap = new Map();
        //fill tempMap
        this.heapfile.insertMap(tempMap.getMapByteArray());








    }
}
