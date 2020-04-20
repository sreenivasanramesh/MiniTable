package bigTest;

import BigT.*;
import bufmgr.*;

import cmdline.MiniTable;
import cmdline.Utils;
import global.GlobalConst;
import global.SystemDefs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static global.GlobalConst.NUMBUF;

public class joinTest extends MiniTable {

    public static void main(String[] args) throws Exception {

        //batchinsert DATAFILENAME TYPE BIGTABLENAME
//        String dataFile = "/home/ganesh/Documents/Documents/DBMSI/project_phase_2/project2_testdata1.csv";
////        short[] BIGT_STR_SIZES = setBigTConstants(dataFile);
        Integer type = Integer.parseInt("1");
//        String tableName = "ganesh1";
////        checkDBExists(tableName);
//        // Set the metadata name for the given DB. This is used to set the headers for the Maps
//        File file = new File("/tmp/" + tableName + "_metadata.txt");
//        FileWriter fileWriter = new FileWriter(file);
//        BufferedWriter bufferedWriter =
//                new BufferedWriter(fileWriter);
//        bufferedWriter.write(dataFile);
//        bufferedWriter.close();
//        Utils.batchInsert(dataFile, tableName, type);
//        // insert two
//
//
//        //batchinsert DATAFILENAME TYPE BIGTABLENAME
//        dataFile = "/home/ganesh/Documents/Documents/DBMSI/project_phase_2/project2_testdata2.csv";
////        BIGT_STR_SIZES = setBigTConstants(dataFile);
//        tableName = "ganesh2";
////        checkDBExists(tableName);
//        // Set the metadata name for the given DB. This is used to set the headers for the Maps
//        file = new File("/tmp/" + tableName + "_metadata.txt");
//        fileWriter = new FileWriter(file);
//        bufferedWriter = new BufferedWriter(fileWriter);
//        bufferedWriter.write(dataFile);
//        bufferedWriter.close();
//        Utils.batchInsert(dataFile, tableName, type);

        rowJoin rj;
        String colName = "Anatidae";
        new SystemDefs(Utils.getDBPath("ganesh"), Utils.NUM_PAGES, NUMBUF, "Clock");
        rj = new rowJoin(10, new Stream(new bigT("ganesh1"), type, "*", colName, "*"), "ganesh2", colName, "res_ganesh");
        Utils.query("res_ganesh", type, 1, "*", "*", "*", NUMBUF);
    }
}
