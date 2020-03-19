package BigT;

import btree.IndexFileScan;
import global.*;

import heap.*;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;


public class Stream extends Scan {

    /**
     * Initialize a stream of maps on big-table
     */
    Heapfile heapfile;
    String rowFilter;
    String columnFilter;
    String valueFilter;
    bigT bigTable;
    int orderType;
    int type;
    MID mid = null;
    String starFilter = new String("*");
    String rangeRegex = new String("\\[\\d+, \\d+\\]");
    private IndexFileScan btIndScan;


    /* Pending */
    public Stream(bigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter, int type) throws Exception {
        super(bigtable.heapfile);

        int mapCount = bigtable.getMapCnt();
        int rowCount = bigtable.getRowCnt();
        int columnCount = bigtable.getColumnCnt();
        this.bigTable = bigtable;
        this.orderType = orderType;
        this.heapfile = bigtable.heapfile;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        this.type = type;

    }


    /**
     * Closes open stream. Same as Heap.Scan.closeScan.
     */
    public void closeStream() {
        super.closescan();
    }


    /**
     * Retrieve the next map in the stream
     */
    public Map getNext(MID mid) throws Exception {
        Map mapObj = null;
        if (!nextRecordExists) {
            nextDataPage();
        }

        if (datapage == null ) return null;

        mid = new MID(nextRid.pageNo, nextRid.slotNo);
        try {
            mapObj = datapage.getMap(mid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        MID nextMapId = datapage.nextMap(mid);
        if (nextMapId == null ) {
            nextRecordExists = false;
        } else {
            nextRecordExists = true;
            nextRid = new RID(nextMapId.getPageNo(), nextMapId.getSlotNo());
        }
        return mapObj;
    }

    private CondExpr[] getCondExpr(String filter){
        if (filter.equals("*")){
            return null;
        }
        else if (filter.contains(",")){
            String[] range = filter.replaceAll("[\\[ \\]]", "").split(",");
            //cond expr of size 3 for range searches
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].operand2.string = range[0];
            expr[0].next = null;
            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopLE);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[1].operand2.string = range[1];
            expr[1].next = null;
            expr[2] = null;
            return expr;
        }
        else{
            //equality search
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].operand2.string = filter;
            expr[0].next = null;
            expr[1] = null;
            return expr;
        }
    }
}