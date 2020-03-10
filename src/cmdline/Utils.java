package cmdline;

import global.MID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;
import bufmgr.*;
import global.AttrType;
import BigT.*;
import diskmgr.pcounter;

import java.io.*;
import static global.GlobalConst.NUMBUF;

class Utils {

    private static final int NUM_PAGES = 5000;

    static void batchInsert(String dataFile, String tableName, int type) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException
    {
        String dbpath =  "~/" + tableName + "-db";
        File f = new File(dbpath);
        //If DB exists use it, else create a new DB with NUM_PAGES pages
        Integer numPages = !f.exists() ? NUM_PAGES : 0;
        SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "Clock");

        FileInputStream fileStream = null;
        BufferedReader br = null;
        try
        {
            //bigT bigTable = new bigT(tableName, type);
            fileStream = new FileInputStream(dataFile);
            br = new BufferedReader(new InputStreamReader(fileStream));
            String inputStr;
            int mapCount = 0;

            while ((inputStr = br.readLine()) != null)
            {
                String[] input = inputStr.split("\t");
                Map map = new Map();
                short[] strSizes1 = new short[]{(short) input[0].getBytes().length,  //rowValue
                                                (short) input[1].getBytes().length,  //colValue
                                                (short) input[3].getBytes().length}; //keyValue
                AttrType[] attrType = new AttrType[] {new AttrType(0), new AttrType(1), new AttrType(1)};

                //set map
                //map.setHeader(attrType, strSizes1); //TODO: have to pull changes from devil
                //map.setHeader((short)strSizes1, attrType); //have to pull changes from devil); //have to pull changes from devil
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);

                Heapfile heapfile = new Heapfile("/Users/rakeshr/test.db");
                //
                // TODO replace with bigT.insertMap()
                //MID mid = heapfile.insertMap(map.getMapByteArray());
                mapCount++;
            }
            System.out.println(mapCount + " tuples inserted...\n");

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
        System.out.println("Reads:  " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);


    }
}
