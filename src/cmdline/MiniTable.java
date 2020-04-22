package cmdline;

import bufmgr.*;
import global.AttrType;
import global.SystemDefs;

import java.io.*;


public class MiniTable {
    public static final AttrType[] BIGT_ATTR_TYPES = new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)};
    public static short[] BIGT_STR_SIZES = new short[]{(short) 21,  //rowValue
            (short) 21,  //colValue
            (short) 21}; //keyValue;
    public static int orderType = 1;
    public static boolean mapInsertOrder = false;
    public static int insertType = 0;
    
    public static void main(String[] args) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        
        String input = null;
        String[] inputStr = null;
        while (true) {
            System.out.print("miniTable>  ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            input = br.readLine();
            if (input.equals(""))
                continue;
            inputStr = input.trim().split("\\s+");
            final long startTime = System.currentTimeMillis();

            try {
                if (inputStr[0].equalsIgnoreCase("exit"))
                    break;
                else if (inputStr[0].equalsIgnoreCase("batchinsert")) {
                    //batchinsert DATAFILENAME TYPE BIGTABLENAME
                    String dataFile = inputStr[1];
                    /*
                    Setting constant size strings of 25 bytes
                    */
//                    BIGT_STR_SIZES = setBigTConstants(dataFile);
                    Integer type = Integer.parseInt(inputStr[2]);
                    String tableName = inputStr[3];
//                    checkDBExists(tableName);
                    // Set the metadata name for the given DB. This is used to set the headers for the Maps
                    File file = new File("/tmp/" + tableName + "_metadata.txt");
                    FileWriter fileWriter = new FileWriter(file);
                    BufferedWriter bufferedWriter =
                            new BufferedWriter(fileWriter);
                    bufferedWriter.write(dataFile);
                    bufferedWriter.close();
                    Utils.batchInsert(dataFile, tableName, type);
                } else if (inputStr[0].equalsIgnoreCase("query")) {

                    //query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
                    String tableName = inputStr[1].trim();
                    String filename = "/tmp/" + tableName + "_metadata.txt";

                    FileReader fileReader;
                    BufferedReader bufferedReader = null;
                    try {
                        fileReader = new FileReader(filename);
                        bufferedReader = new BufferedReader(fileReader);
                    } catch (FileNotFoundException e) {
                        System.out.println("Given tableName does not exist\n\n");
                        continue;
                    }
                    String metadataFile = bufferedReader.readLine();
                    // Always close files.
                    bufferedReader.close();
//                    BIGT_STR_SIZES = setBigTConstants(metadataFile);
                    orderType = Integer.parseInt(inputStr[2]);
                    String rowFilter = inputStr[3].trim();
                    String colFilter = inputStr[4].trim();
                    String valFilter = inputStr[5].trim();
                    Integer NUMBUF = Integer.parseInt(inputStr[6]);
                    Utils.query(tableName, orderType, rowFilter, colFilter, valFilter, NUMBUF);
                }
                else if (inputStr[0].equalsIgnoreCase("rowsort")) {
                    //rowsort INBTNAME OUTBTNAME COLUMNNAME NUMBUF
                    //rowSort(String inTableName, String outTableName, String columnName, int NUMBUF) throws Exception
                    String inTableName = inputStr[1].trim();
                    String outTableName = inputStr[2].trim();
                    String columnName = inputStr[3].trim();
                    Integer NUMBUF = Integer.parseInt(inputStr[4].trim());
                    Utils.rowSort(inTableName, outTableName, columnName, NUMBUF);

                }
                else {
                    System.out.println("Invalid input. Type exit to quit.\n\n");
                    continue;
                }
            } catch (Exception e) {
                System.out.println("Invalid parameters. Try again.\n\n");
                continue;
            }
            SystemDefs.JavabaseBM.flushAllPages();

            final long endTime = System.currentTimeMillis();
            System.out.println("Total execution time: " + (endTime - startTime) / 1000.0 + " seconds");


        }

        System.out.print("exiting...");
    }

    private static short[] setBigTConstants(String dataFileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(dataFileName))) {
            String line;
            int maxRowKeyLength = Short.MIN_VALUE;
            int maxColumnKeyLength = Short.MIN_VALUE;
            int maxValueLength = Short.MIN_VALUE;
            int maxTimeStampLength = Short.MIN_VALUE;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                OutputStream out = new ByteArrayOutputStream();
                DataOutputStream rowStream = new DataOutputStream(out);
                DataOutputStream columnStream = new DataOutputStream(out);
                DataOutputStream timestampStream = new DataOutputStream(out);
                DataOutputStream valueStream = new DataOutputStream(out);

                rowStream.writeUTF(fields[0]);
                maxRowKeyLength = Math.max(rowStream.size(), maxRowKeyLength);

                columnStream.writeUTF(fields[1]);
                maxColumnKeyLength = Math.max(columnStream.size(), maxColumnKeyLength);

                timestampStream.writeUTF(fields[2]);
                maxTimeStampLength = Math.max(timestampStream.size(), maxTimeStampLength);

                valueStream.writeUTF(fields[3]);
                maxValueLength = Math.max(valueStream.size(), maxValueLength);

            }
            return new short[]{
                    (short) maxRowKeyLength,
                    (short) maxColumnKeyLength,
                    (short) maxValueLength
            };
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return new short[0];
    }
}
