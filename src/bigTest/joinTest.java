package bigTest;

import BigT.*;
import bufmgr.*;

import cmdline.MiniTable;
import cmdline.Utils;
import global.GlobalConst;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static global.GlobalConst.NUMBUF;

public class joinTest extends MiniTable {

    public static void batchInsert(String dataFile, String tableName, Integer type) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        // Set the metadata name for the given DB. This is used to set the headers for the Maps
        File file = new File("/tmp/" + tableName + "_metadata.txt");
        FileWriter fileWriter = new FileWriter(file);
        BufferedWriter bufferedWriter =
                new BufferedWriter(fileWriter);
        bufferedWriter.write(dataFile);
        bufferedWriter.close();
        Utils.batchInsert(dataFile, tableName, type);
    }
    
    public static void getCount(String tableName) throws PageNotFoundException, PagePinnedException, PageUnpinnedException, HashOperationException, ReplacerException, BufMgrException, InvalidFrameNumberException, IOException, HashEntryNotFoundException, InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        new SystemDefs(Utils.getDBPath(Utils.getDBPath("ganesh")), Utils.NUM_PAGES, NUMBUF, "Clock");
        bigT bigT = new bigT(tableName);
        System.out.println("bigT.getMapCnt() = " + bigT.getMapCnt());
        bigT.close();
    }
    
    public static void rowJoin(Integer type) throws Exception {
        rowJoin rj;
        String colName = "Zebra";
        new SystemDefs(Utils.getDBPath(Utils.getDBPath("ganesh")), Utils.NUM_PAGES, NUMBUF, "Clock");

//        rj = new rowJoin(10, new Stream(new bigT("ganesh1"), type, "*", colName, "*"), "ganesh2", colName, "res_ganesh");
        Stream leftstream = new bigT("ganesh1").openStream(1, "*", colName, "*");
        rj = new rowJoin(10, leftstream, "ganesh2", colName, "ash5");
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        Utils.query("ash5", type, 1, "*", "*", "*", NUMBUF);
    }
    
    public static void main(String[] args) throws Exception {

//        batchinsert DATAFILENAME TYPE BIGTABLENAME
        
        // batch insert 1
        Integer type = Integer.parseInt("1");

//        String dataFile = "/Users/sumukhashwinkamath/Downloads/test/ts1.csv";
//////         String dataFile = "/home/ganesh/Documents/Documents/DBMSI/phase3/as1.csv";
//        String tableName = "ganesh1";
//        batchInsert(dataFile, tableName, type);
//        getCount(tableName);
////
////        // batch insert 2
//          dataFile = "/Users/sumukhashwinkamath/Downloads/test/ts2.csv";
//////        dataFile = "/home/ganesh/Documents/Documents/DBMSI/phase3/as1.csv";
//        tableName = "ganesh2";
//        batchInsert(dataFile, tableName, type);
//        getCount(tableName);

        rowJoin(type);
        
    }
}
