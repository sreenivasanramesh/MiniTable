package cmdline;

import global.MID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;
import bufmgr.*;
import global.*;
import BigT.*;
import diskmgr.pcounter;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;

import java.io.*;
import static global.GlobalConst.NUMBUF;

class Utils {

    private static final int NUM_PAGES = 10000;

    static void batchInsert(String dataFile, String tableName, int type) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException
    {
        String dbPath =  getDBPath(tableName);
        System.out.println(dbPath);
        File f = new File(dbPath);
        //If DB exists use it, else create a new DB with NUM_PAGES pages
        Integer numPages = !f.exists() ? NUM_PAGES : 0;
        //SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "LRU");
        new SystemDefs(dbPath, numPages, NUMBUF, "Clock");

        FileInputStream fileStream = null;
        BufferedReader br = null;
        try
        {
            //bigT bigTable = new bigT(tableName, type);
            Heapfile heapfile = new Heapfile(tableName + ".heap");
            fileStream = new FileInputStream(dataFile);
            br = new BufferedReader(new InputStreamReader(fileStream));
            String inputStr;
            int mapCount = 0;

            while ((inputStr = br.readLine()) != null)
            {
                String[] input = inputStr.split(",");
                //set the map
                Map map = new Map();
                short[] strSizes1 = new short[]{(short) input[0].getBytes().length,  //rowValue
                                                (short) input[1].getBytes().length,  //colValue
                                                (short) input[3].getBytes().length}; //keyValue
                AttrType[] attrType = new AttrType[] {new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)};
                map.setHeader(attrType, strSizes1);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);

                //
                // TODO replace with bigT.insertMap()
                MID mid = heapfile.insertMap(map.getMapByteArray());
                mapCount++;
            }
            System.out.println(mapCount + " tuples inserted...\n");
            System.out.println("tuple count: " + heapfile.getRecCnt());


        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            fileStream.close();
            br.close();
        }

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
    }




    static void query(String tableName, Integer type, Integer orderType, String rowFilter, String colFilter, String valFilter, Integer NUMBUF) throws Exception {
        String dbPath =  getDBPath(tableName);
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        int resultCount = 0;

        try {
            //TODO: query logic
            bigT bigTable = new bigT(tableName, type);
            Stream mapStream = bigTable.openStream(orderType, rowFilter, colFilter, valFilter);

            MID mapId = null;

            while (true) {
                //TODO: I'm not really sure about the mapId, have to check how to do this
                Map mapObj = mapStream.getNext(mapId);
                if (mapObj == null)
                    break;
                mapObj.print();
                resultCount++;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Matched Records: " + resultCount);
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
    }




    public static String getDBPath(String tableName){
        return "/Users/vasan/" + tableName + ".db";
    }




    static CondExpr[] getCondExpr(String filter){
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


