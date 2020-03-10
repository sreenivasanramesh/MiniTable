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

public class BatchInsert {

    private static final int NUM_PAGES = 5000;


    public static void main(String[] args) throws Exception{

        /*8 Maybe use one CLI for batchinsert and querying?
        //BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
        //Integer type = Integer.parseInt(args[1]);
        //Integer orderType = Integer.parseInt(args[2]);
        //String rowFilter = args[3];
        //String colFilter = args[4];
        //String valFilter = args[5];
        //Integer NUMBUF = Integer.parseInt(args[6]);
         */

        //batchinsert DATAFILENAME TYPE BIGTABLENAME
        String dataFile = args[1];
        Integer type = Integer.parseInt(args[2]);
        String tableName = args[3];


        String dbpath =  "~/" + tableName + "-db";
        File f = new File(dbpath);
        //If DB exists use it, else create a new DB with NUM_PAGES pages
        Integer numPages = !f.exists() ? NUM_PAGES : 0;
        SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "Clock");

        //batch insert
        commandLine(dataFile, tableName, type);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Reads: " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
    }


    private static void commandLine(String dataFile, String tableName, int type) throws IOException
    {
        FileInputStream fileStream = null;
        BufferedReader br = null;
        try
        {
            bigT bigTable = new bigT(tableName, type);
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
                map.setHeader(attrType, strSizes1); //have to pull changes from devil
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);

                Heapfile hf = new Heapfile("/Users/rakeshr/test.db");
                //TODO replace with bigT.insertMap()
                MID mid = hf.insertMap(map.getMapByteArray());
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
    }
}
