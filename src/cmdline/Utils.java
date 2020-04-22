package cmdline;

import BigT.Map;
import BigT.Stream;
import BigT.bigT;
import BigT.rowJoin;
import commonutils.EvictingQueue;
import diskmgr.pcounter;
import global.*;
import heap.*;
import iterator.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static global.GlobalConst.NUMBUF;

public class Utils {
    
    public static final int NUM_PAGES = 100000;
    
    public static void batchInsert(String dataFile, String tableName, int type, int numBufs) throws Exception {
        String UTF8_BOM = "\uFEFF";
        String dbPath = getDBPath();
        System.out.println("DB name =>" + dbPath);
        File f = new File(dbPath);
        Integer numPages = NUM_PAGES;
        new SystemDefs(dbPath, numPages, numBufs, "Clock");
        pcounter.initialize();
        
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
    
                if (input[0].length() > MiniTable.BIGT_STR_SIZES[0]) {
                    input[0] = input[0].substring(0, MiniTable.BIGT_STR_SIZES[0]);
                }
                if (input[1].length() > MiniTable.BIGT_STR_SIZES[1]) {
                    input[1] = input[1].substring(0, MiniTable.BIGT_STR_SIZES[1]);
                }
                if (input[2].length() > MiniTable.BIGT_STR_SIZES[2]) {
                    input[2] = input[2].substring(0, MiniTable.BIGT_STR_SIZES[2]);
                }
                if (input[0].startsWith(UTF8_BOM)) {
                    input[0] = input[0].substring(1).trim();
                }
    
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[3]));
                map.setValue(input[2]);
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
                MiniTable.orderType = 1;
                sort = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, fscan, 1, new TupleOrder(TupleOrder.Ascending), 20, 25, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Heapfile duplicateRemoved = new Heapfile(tableName + "_duplicate_removed");
            Map m = sort.get_next();
            // TODO: Add duplicate elimination logic
            String oldRowLabel = null;
            String oldColumnLabel = null;
            if (m != null) {
                oldRowLabel = m.getRowLabel();
                oldColumnLabel = m.getColumnLabel();
            }
            EvictingQueue<Map> evictingQueue = new EvictingQueue<>(3);
            FileWriter fileWriter = new FileWriter("/tmp/resultsash");
            int count = 1;
            while (m != null) {
//                if ((!oldRowLabel.equals(m.getRowLabel()) && oldColumnLabel.equals(m.getColumnLabel())) || ((oldRowLabel.equals(m.getRowLabel()) && !oldColumnLabel.equals(m.getColumnLabel()))) || ((oldRowLabel.equals(m.getRowLabel()) && oldColumnLabel.equals(m.getColumnLabel())))) {
                if (!oldRowLabel.equals(m.getRowLabel()) || !oldColumnLabel.equals(m.getColumnLabel())) {
                    //Heap push evicting queue
                    for (Map map : evictingQueue) {
                        count += 1;
                        fileWriter.write(map.getRowLabel() + "," + map.getColumnLabel() + "," + map.getTimeStamp() + "," + map.getValue() + "\n");
                        duplicateRemoved.insertMap(map.getMapByteArray());
                    }
                    evictingQueue.clear();
                }
                Map insertMap = new Map();
                insertMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
                insertMap.copyMap(m);
                evictingQueue.add(insertMap);
                oldRowLabel = new String(m.getRowLabel());
                oldColumnLabel = new String(m.getColumnLabel());
                m = sort.get_next();
            }

//            System.out.println("evict len:");
//            System.out.println(evictingQueue.size());
            for (Map map : evictingQueue) {
                count += 1;
                fileWriter.write(map.getRowLabel() + "," + map.getColumnLabel() + "," + map.getTimeStamp() + "," + map.getValue() + "\n");
                duplicateRemoved.insertMap(map.getMapByteArray());
            }
            System.out.println("count = " + count);
            fileWriter.close();
            evictingQueue.clear();
    
    
            System.out.println("duplicateRemoved.getRecCnt() = " + duplicateRemoved.getRecCnt());
            bigTable.batchInsert(duplicateRemoved, type);
            duplicateRemoved.deleteFile();
    
            System.out.println("Final Records =>");
            for (int i = 0; i < 5; i++) {
                System.out.println("===========================");
                System.out.println("Heapfile " + (i + 1));
                System.out.println("===========================");
                MapScan mapScan = bigTable.heapfiles[i].openMapScan();
                MID mid = new MID();
                Map map = mapScan.getNext(mid);
                while (map != null) {
                    map.print();
                    map = mapScan.getNext(mid);
                }
                mapScan.closescan();
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
            assert fscan != null;
            fscan.close();
            hf.deleteFile();
            sort.close();
            bigTable.close();
    
    
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileStream.close();
            br.close();
        }
        
        SystemDefs.JavabaseBM.setNumBuffers(0);
//        SystemDefs.JavabaseBM.flushAllPages();
//        SystemDefs.JavabaseDB.closeDB();
    }
    
    
    public static void query(String tableName, Integer orderType, String rowFilter, String colFilter, String valFilter, Integer NUMBUF) throws Exception {
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
    
    public static void rowJoinWrapper(int numBuf, String btName1, String btName2, String outBtName, String columnFilter) throws Exception {
        rowJoin rj;
        new SystemDefs(Utils.getDBPath(), Utils.NUM_PAGES, numBuf, "Clock");
        
        Stream leftstream = new bigT(btName1, false).openStream(1, "*", columnFilter, "*");
        rj = new rowJoin(20, leftstream, btName2, columnFilter, outBtName, btName1);
        SystemDefs.JavabaseBM.setNumBuffers(0);
        System.out.println("Query results => ");
        Utils.query(outBtName, 1, "*", "*", "*", NUMBUF);
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
    
    public static void addTableToInventory(String bigTable) throws Exception {
        try {
            Heapfile inventory = new Heapfile("bigT_inventory");
            Map tableInfo = new Map();
            tableInfo.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
            tableInfo.setRowLabel(bigTable);
            tableInfo.setColumnLabel("0");
            tableInfo.setTimeStamp(0);
            tableInfo.setValue("0");
            inventory.insertMap(tableInfo.getMapByteArray());
        } catch (Exception exp) {
            exp.printStackTrace();
            throw new Exception("Fetching from Inventory failed " + exp.toString());
        }
    }
    
    public static List<String> getAllTablesInventory() throws InvalidTupleSizeException, IOException, HFDiskMgrException, HFBufMgrException, HFException {
        List<String> bigTableList = new ArrayList<>();
        Heapfile bigTInventory = new Heapfile("bigT_inventory");
        MapScan mapScan = bigTInventory.openMapScan();
        MID mid = new MID();
        Map m = mapScan.getNext(mid);
        while (m != null) {
            bigTableList.add(m.getRowLabel());
            mid = new MID();
            m = mapScan.getNext(mid);
        }
        mapScan.closescan();
        return bigTableList;
    }
    
    public static void getCounts(Integer numBufs) throws Exception {
        try {
            new SystemDefs(Utils.getDBPath(), Utils.NUM_PAGES, numBufs, "Clock");
            List<String> tables = getAllTablesInventory();
            System.out.println("================================");
            for (String table : tables) {
                System.out.println("Big Table Name: " + table);
                System.out.println("----------------------------");
                bigT bigT = new bigT(table, false);
                System.out.println("MapCount: " + bigT.getMapCnt());
                System.out.println("RowCount: " + bigT.getRowCnt());
                System.out.println("ColCount: " + bigT.getColumnCnt());
                System.out.println("----------------------------");
                bigT.close();
            }
            System.out.println("================================");
        } catch (Exception exp) {
            exp.printStackTrace();
            throw new Exception("Error while getting counts : " + exp.toString());
        }
        
    }
}


