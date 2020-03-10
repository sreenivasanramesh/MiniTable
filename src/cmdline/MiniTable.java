package cmdline;

import bufmgr.*;
import diskmgr.pcounter;
import global.SystemDefs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class MiniTable {

    public static void main(String[] args) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {

        String input = null;
        String[] inputStr = null;
        while(true){
            System.out.print("miniTable>  ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            input = br.readLine();
            if (input.equals(""))
                continue;
            inputStr = input.split("\\s+");

            try {
                if (inputStr[0].equalsIgnoreCase("exit"))
                    break;
                else if (inputStr[0].equalsIgnoreCase("batchinsert")) {
                    String dataFile = inputStr[1];
                    Integer type = Integer.parseInt(inputStr[2]);
                    String tableName = inputStr[3];
                    Utils.batchInsert(dataFile, tableName, type);
                } else if (inputStr[0].equalsIgnoreCase("query")) {
                /*
                //BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
                //Integer type = Integer.parseInt(args[1]);
                //Integer orderType = Integer.parseInt(args[2]);
                //String rowFilter = args[3];
                //String colFilter = args[4];
                //String valFilter = args[5];
                //Integer NUMBUF = Integer.parseInt(args[6]);
                */
                }
                else
                    System.out.println("Invalid input. Type exit to quit.\n\n");
            }
            catch (Exception e){
                System.out.println("Invalid parameters. Try again.\n\n");
            }


        }

        System.out.print("exiting...");
        //SystemDefs.JavabaseBM.flushAllPages();
        //SystemDefs.JavabaseDB.closeDB();
        //System.out.println("Reads: " + pcounter.rcounter);
        //System.out.println("Writes: " + pcounter.wcounter);
    }

}
