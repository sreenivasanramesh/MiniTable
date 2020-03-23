package cmdline;

import BigT.Map;
import BigT.Stream;
import BigT.bigT;
import bufmgr.*;
import diskmgr.pcounter;
import global.AttrOperator;
import global.AttrType;
import global.MID;
import global.SystemDefs;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.*;

import static global.GlobalConst.NUMBUF;

class Utils {

    private static final int NUM_PAGES = 100000;

    static void batchInsert(String dataFile, String tableName, int type) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        //String dbPath = getDBPath(tableName, type);
        String dbPath = getDBPath(tableName);
        System.out.println(dbPath);
        File f = new File(dbPath);
        //If DB exists use it, else create a new DB with NUM_PAGES pages
//        Integer numPages = !f.exists() ? NUM_PAGES : 0;
        Integer numPages = NUM_PAGES;
        //SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "LRU");
//        new SystemDefs(dbPath, numPages, NUMBUF, "Clock");
        new SystemDefs(dbPath, numPages, NUMBUF, "Clock");
        pcounter.initialize();

        FileInputStream fileStream = null;
        BufferedReader br = null;
        try {
            bigT bigTable = new bigT(tableName, type);
            fileStream = new FileInputStream(dataFile);
            br = new BufferedReader(new InputStreamReader(fileStream));
            String inputStr;
            int mapCount = 0;

            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                //set the map
                Map map = new Map();
                map.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);
                MID mid = bigTable.insertMap(map.getMapByteArray());
                mapCount++;
            }
            System.out.println("\n=======================================\n");
            System.out.println(mapCount + " maps inserted...\n");
            System.out.println("map count: " + bigTable.getMapCnt());
            System.out.println("Distinct Rows = " + bigTable.getRowCnt());
            System.out.println("Distinct Coloumns = " + bigTable.getColumnCnt());
            System.out.println("\n=======================================\n");
            System.out.println("Reads : " + pcounter.rcounter);
            System.out.println("Writes: " + pcounter.wcounter);
            System.out.println("NumBUFS: " + NUMBUF);
            System.out.println("\n=======================================\n");
            bigTable.close();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileStream.close();
            br.close();
        }

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
    }


    static void query(String tableName, Integer type, Integer orderType, String rowFilter, String colFilter, String valFilter, Integer NUMBUF) throws Exception {
        //String dbPath = getDBPath(tableName, type);
        String dbPath = getDBPath(tableName);
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();
        int resultCount = 0;

        try {

            bigT bigTable = new bigT(tableName);
            if (!type.equals(bigTable.getType())) {
                System.out.println("Type Mismatch");
                bigTable.close();
                return;
            }
            Stream mapStream = bigTable.openStream(orderType, rowFilter, colFilter, valFilter);

            MID mapId = null;

            while (true) {
                Map mapObj = mapStream.getNext();
                if (mapObj == null)
                    break;
                mapObj.print();
                resultCount++;
            }
            bigTable.close();
            mapStream.closeStream();

        } catch (Exception e) {
            e.printStackTrace();
        }
    
        System.out.println("\n=======================================\n");
        System.out.println("Matched Records: " + resultCount);
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("\n=======================================\n");
        
    }


//    public static String getDBPath(String tableName, Integer type) {
//        return "/tmp/" + tableName + "." + type + ".db";
//    }

    public static String getDBPath(String tableName) {
        return "/tmp/" + tableName  + ".db";
    }


    static CondExpr[] getCondExpr(String filter) {
        if (filter.equals("*")) {
            return null;
        } else if (filter.contains(",")) {
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
        } else {
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


