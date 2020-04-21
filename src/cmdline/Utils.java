package cmdline;

import BigT.Map;
import BigT.Stream;
import BigT.bigT;
import bufmgr.*;
import diskmgr.pcounter;
import global.*;
import heap.*;
import iterator.*;

import java.io.*;

import static global.GlobalConst.NUMBUF;

class Utils {

    private static final int NUM_PAGES = 100000;

    static void batchInsert(String dataFile, String tableName, int type) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException, HFDiskMgrException, HFBufMgrException, HFException {
        String dbPath = getDBPath();
        System.out.println("DB name =>" + dbPath);
        File f = new File(dbPath);
        Integer numPages = NUM_PAGES;
        new SystemDefs(dbPath, numPages, NUMBUF, "Clock");
        pcounter.initialize();
        String UTF8_BOM = "\uFEFF";

        FileInputStream fileStream = null;
        BufferedReader br = null;
        Heapfile hf = new Heapfile(tableName + "tempfile");
        try {
            bigT bigTable = new bigT(tableName, true);
            fileStream = new FileInputStream(dataFile);
            br = new BufferedReader(new InputStreamReader(fileStream));
            String inputStr;
            int mapCount = 0;

            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                //set the map
                Map map = new Map();
                map.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
                
                if (input[0].length() > 25) {
                    input[0] = input[0].substring(0, 25);
                }
                if(input[0].startsWith(UTF8_BOM)){
                    input[0] = input[0].substring(1).trim();
                }
                if (input[1].length() > 25) {
                    input[1] = input[1].substring(0, 25);
                }
                if (input[3].length() > 25) {
                    input[3] = input[3].substring(0, 25);
                }
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);
                hf.insertMap(map.getMapByteArray());
                mapCount++;
            }
    
            FldSpec[] projlist = new FldSpec[4];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);
            projlist[2] = new FldSpec(rel, 3);
            projlist[3] = new FldSpec(rel, 4);
    
            FileScan fscan = null;

            try {
                fscan = new FileScan(tableName + "tempfile", MiniTable.BIGT_ATTR_TYPES,
                        MiniTable.BIGT_STR_SIZES, (short) 4, 4, projlist, null);
            } catch (Exception e) {
                e.printStackTrace();
            }

            MapSort sort = null;
            try {
                sort = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, fscan, 1, new TupleOrder(TupleOrder.Ascending), 10, 25, true);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Map m = sort.get_next();
            while(m != null){
//                m.print();
                m = sort.get_next();
            }
    
            
            
            System.out.println("Sorting done");
            System.out.println("hf.getRecCnt() = " + hf.getRecCnt());
            bigTable.batchInsert(hf, type);
    
    
            System.out.println("Final Records =>");
            for(int i=0;i<5;i++){
                System.out.println("===========================");
                System.out.println("Heapfile " + i);
                System.out.println("===========================");
                MapScan mapScan = bigTable.heapfiles[i].openMapScan();
                MID mid = new MID();
                Map map = mapScan.getNext(mid);
                while(map != null){
                    map.print();
                    map = mapScan.getNext(mid);
                }
            }
            
            System.out.println("=======================================\n");
            System.out.println("map count: " + bigTable.getMapCnt());
            System.out.println("Distinct Rows = " + bigTable.getRowCnt());
            System.out.println("Distinct Coloumns = " + bigTable.getColumnCnt());
            System.out.println("\n=======================================\n");
            System.out.println("Reads : " + pcounter.rcounter);
            System.out.println("Writes: " + pcounter.wcounter);
            System.out.println("NumBUFS: " + NUMBUF);
            System.out.println("\n=======================================\n");
            hf.deleteFile();
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
        String dbPath = getDBPath();
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();
        int resultCount = 0;

        try {

            bigT bigTable = new bigT(tableName, false);
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

    public static String getDBPath() {
        String useId = "user.name";
        String userAccName;
        userAccName = System.getProperty(useId);
        return "/tmp/" + userAccName + ".db";
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


