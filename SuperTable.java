import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

    public static final String POWERSTABLE_NAME = "powers";
    public static final String COLUMNFAMILIY_PERSONAL = "personal";
    public static final String COLUMNFAMILIY_PROFESSIONAL = "professional";

    private static Configuration mp4Config = HBaseConfiguration.create();

    private static HBaseAdmin hbaseAdmin;

    private static void init()  {
        // Instaniate HBaseAdmin class
        try {
            hbaseAdmin =  new HBaseAdmin(mp4Config);
        } catch (IOException e) {
            handleCriticalError("Could not create HBase admin", e);
        }

        // Execute the table through admin
        instantiateTableDescriptor();

    }


    public static void main(String[] args) throws IOException {

        init();

        // Instantiating HTable class
        HTable table = getOrCreateTable(POWERSTABLE_NAME);

        // Repeat these steps as many times as necessary
        table.put(rowOf("row1", "superman", "strength", "clark", "100"));
        table.put(rowOf("row2", "batman", "money", "bruce", "50"));
        table.put(rowOf("row3", "wolverine", "healing", "logan", "75"));

        // Close table
        table.close();

        // Instantiate the Scan class
        Scan scanner = new Scan();

        // Scan the required columns
        scanner.addColumn(Bytes.toBytes(COLUMNFAMILIY_PERSONAL), Bytes.toBytes("hero"));

        // Get the scan result
        ResultScanner results = table.getScanner(scanner);

        // Read values from scan result
        for(Result row = results.next(); null != row; results.next()) {
            // Print scan result
            printRow(row);
        }

        // Close the scanner
        results.close();

        // Htable closer

    }

    private static void printRow(Result row) {
        System.out.println(row.toString());
    }

    public static Put rowOf(String rowId, String hero, String power, String name, String xp) {
        Put put = new Put(Bytes.toBytes(rowId));
        put.add(Bytes.toBytes(COLUMNFAMILIY_PERSONAL), Bytes.toBytes(hero), Bytes.toBytes(power));
        put.add(Bytes.toBytes(COLUMNFAMILIY_PROFESSIONAL), Bytes.toBytes(name), Bytes.toBytes(xp));
    }

    private static void instantiateTableDescriptor() {
        if(checkIfTableDoesNotExist(POWERSTABLE_NAME)) {
            try {
                hbaseAdmin.createTable(createTableDescriptor());
            } catch (IOException e) {
                throw new RuntimeException("Could not create table descriptor", e);
            }
        }
    }

    private static boolean checkIfTableDoesNotExist(String tableName) {
        try {
            return !hbaseAdmin.tableExists(tableName);
        } catch (IOException e) {
            throw new RuntimeException("Error when checking for table", e);
        }
    }

    private static HTableDescriptor createTableDescriptor() {
        // Instantiate table descriptor class
        HTableDescriptor mp4TableDescriptor = new HTableDescriptor(TableName.valueOf(POWERSTABLE_NAME));

        // Add column families to table descriptor
        mp4TableDescriptor.addFamily(new HColumnDescriptor(COLUMNFAMILIY_PERSONAL));
        mp4TableDescriptor.addFamily(new HColumnDescriptor(COLUMNFAMILIY_PROFESSIONAL));

        return mp4TableDescriptor;

    }

    private static HTable getOrCreateTable(String tableName) {
        try {
            return new HTable(mp4Config, tableName);
        } catch (IOException e) {
            throw new RuntimeException("Could not access or create table " + tableName, e);
        }
    }

    private static void handleCriticalError(String message, IOException e) {
        throw new RuntimeException(message, e);
    }
}

