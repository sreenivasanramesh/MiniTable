package cmdline;

import bufmgr.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class MiniTable {

    public static void main(String[] args) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {

        String input = null;
        String[] inputStr = null;
        while(true){
            final long startTime = System.currentTimeMillis();

            System.out.print("miniTable>  ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            input = br.readLine();
            if (input.equals(""))
                continue;
            inputStr = input.trim().split("\\s+");

            try {
                if (inputStr[0].equalsIgnoreCase("exit"))
                    break;
                else if (inputStr[0].equalsIgnoreCase("batchinsert"))
                {
                    //batchinsert DATAFILENAME TYPE BIGTABLENAME
                    String dataFile = inputStr[1];
                    Integer type = Integer.parseInt(inputStr[2]);
                    String tableName = inputStr[3];
                    boolean useMetadata = Boolean.parseBoolean(inputStr[4]);
                    Utils.batchInsert(dataFile, tableName, type, useMetadata);
                }
                else if (inputStr[0].equalsIgnoreCase("query"))
                {
                    //query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
                    String tableName = inputStr[1].trim();
                    Integer type = Integer.parseInt(inputStr[2]);
                    Integer orderType = Integer.parseInt(inputStr[3]);
                    String rowFilter = inputStr[4].trim();
                    String colFilter = inputStr[5].trim();
                    String valFilter = inputStr[6].trim();
                    //String filter = rowFilter + ";" + colFilter + ";" + valFilter;
                    Integer NUMBUF = Integer.parseInt(inputStr[7]);
                    //CondExpr filters[] = Utils.getCondExpr(filter);
                    Utils.query(tableName, type, orderType, rowFilter, colFilter, valFilter, NUMBUF);
                } else
                    System.out.println("Invalid input. Type exit to quit.\n\n");
            } catch (Exception e) {
                System.out.println("Invalid parameters. Try again.\n\n");
                e.printStackTrace();
            }

            final long endTime = System.currentTimeMillis();
            System.out.println("Total execution time: " + (endTime - startTime) / 1000.0);


        }

        System.out.print("exiting...");
    }

}
