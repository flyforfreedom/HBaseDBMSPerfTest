/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.hbasedbmsperftest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author md186047
 */
public class TestHBaseDriver implements TestDriver {

    private Configuration conf = null;
    private Connection databaseConnection = null;
    private ControllerService service;
    private TestParameters testParameters;
    private TestCase testCase;

    private final String testDatabase = "default";
    private final String testTableName = "hbasePerfTest";
    private final String familyName = "payload";
    private final String colName = "c1";

    private Table testTable;
    
    private int batchCount = 0;
    private List<Put> batchPuts; 
    
    private final static int BATCH_SIZE = 100;


    @Override
    public String getName() {
        return "HBase";
    }

    @Override
    public boolean initialise(ControllerService service, TestParameters testParameters) {

        this.service = service;
        this.testParameters = testParameters;

        try {
            conf = HBaseConfiguration.create();
            service.logMessage("Configuration before: " + conf);
            conf.clear();
            //conf.addResource("com/teradata/hbase/resource/hbase-site.xml");

            //mock up hbase-site.xml file
            conf.set("hbase.zookeeper.property.dataDir", "/home/hadoop/software/hbase-1.2.4/zookeeper");
            conf.set("hbase.zookeeper.property.quorum", "localhost");

            conf.set("hbase.rootdir", "hdfs://hbase:9000/hbase");
            conf.set("hbase.zookeeper.quorum", "hbase");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            //        conf.set("hbase.master", "hbase:60010");

            service.logMessage("Configuration after: " + conf);
            HBaseAdmin.checkHBaseAvailable(conf);

            service.logMessage("HBase is available");

            service.logMessage("Creating connection.");
            databaseConnection = ConnectionFactory.createConnection(conf);
            service.logMessage("Connection: " + databaseConnection);

            return true;

        } catch (Throwable t) {
            service.logMessage("Initialisation error: " + t.getMessage());
            t.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean prepareTestCase(TestCase testCase) {
        this.testCase = testCase;
        
        batchPuts = new ArrayList<Put>();
        batchCount = 0 ; 

        try {
            this.testTable = databaseConnection.getTable(TableName.valueOf(testDatabase, testTableName));

        } catch (IOException ex) {
            service.logMessage("Error - Cannot initialize HBase table " + testTableName);
            service.logMessage("IOException - " + ex.getMessage());
            return false;
        }

        if (testCase.equals(TestCase.PUT) && testParameters.isDeleteAllDataFirst()) {

            //cleanup table
            Admin admin = null;
            try {
                service.logMessage("Clean up table - " + testTableName);

                admin = databaseConnection.getAdmin();
                TableName truncateTable = TableName.valueOf(testTableName);

                if (admin.isTableEnabled(truncateTable)) {
                    System.out.print("Table " + testTableName + " was disabled. Enabling it...");
                    admin.disableTable(truncateTable);

                }
                admin.truncateTable(truncateTable, true);
                admin.close();
                service.logMessage("Clean up table - " + testTableName + " Completed.");

                // Instantiating HTable class
                service.logMessage("Initialize Hbase table - " + testTableName);
                this.testTable = databaseConnection.getTable(TableName.valueOf(testDatabase, testTableName));

            } catch (IOException ex) {
                service.logMessage("IOException - " + ex.getMessage());
                return false;
            }
        }

        return true;
    }

    @Override
    public long get(String key) {
        try {

            //service.logMessage("Getting value for key '" + key + "' from HBase.");

            // get data from HBase
            String value = get(testTable, familyName, colName, key);
            return Long.valueOf(value);

        } catch (IOException ex) {
            service.logMessage("IOException - " + ex.getMessage());
            service.logMessage("ERROR - Cannot put table from HBase: " + testDatabase + ":" + testTableName);
            return -1;
        }
    }

    @Override
    public boolean put(String key, long value) {

        if (testParameters.isUseBatchOperations()) {
            return putBatch (key, value);
        }
        return putLiteralSql(key, value);

    }

    /**
     * Insert records one by one
     *
     * @param key
     * @param value
     * @return
     */
    private boolean putLiteralSql(String key, long value) {

        try {
            //service.logMessage("Putting '" + key + "' into HBase for value: " + value);

            put(testTable, familyName, colName, key, Long.toString(value));

            //service.logMessage("Complete putting '" + key + "' into HBase for value: " + value);
            return true;
        } catch (IOException ex) {
            service.logMessage("IOException - " + ex.getMessage());
            service.logMessage("ERROR - Cannot put value into HBase: " + testDatabase + ":" + testTableName);
            return false;
        }
    }

    /**
     * Batch Insert
     * @param key
     * @param value
     * @return 
     */
    private boolean putBatch(String key, long value) {

        try {
            //service.logMessage("Batch Put - Putting '" + key + "' into HBase for value: " + value);

            Put putter = getPutRecord(familyName, colName, key, Long.toString(value));
            
            batchPuts.add(putter);
            batchCount ++;
            
            if(batchCount > BATCH_SIZE) {
                testTable.put(batchPuts);
                //service.logMessage("Complete batch put for " + batchCount + "rows.");
                
                // reset
                batchCount = 0 ;
                batchPuts = new ArrayList<Put>();
                
            }
            return true;
        } catch (IOException ex) {
            service.logMessage("IOException - " + ex.getMessage());
            service.logMessage("ERROR - Cannot put value into HBase: " + testDatabase + ":" + testTableName);
            return false;
        }
    }

    @Override
    public long scan(String key) {

        byte[] prefix = Bytes.toBytes(key);

        Scan scan = new Scan(prefix);
        PrefixFilter prefixFilter = new PrefixFilter(prefix);
        scan.setFilter(prefixFilter);

        ResultScanner resultScanner = null;

        //service.logMessage("About to call Hbase scan");
        try {
            resultScanner = testTable.getScanner(scan);

            long rowCount = countResultSet(resultScanner);
            //service.logMessage("HBase Scan completed. Row count is: " + rowCount);
            return rowCount;

        } catch (IOException ex) {
            service.logMessage("IOException - " + ex.getMessage());
            return -1;
        } finally {
            if (resultScanner != null) {
                resultScanner.close();
            }
        }
    }

    /**
     *
     * @param table
     * @param familyName
     * @param columnName
     * @param key
     * @return
     * @throws IOException
     */
    private String get(Table table, String familyName, String columnName, String key)
            throws IOException {

        Get getter = null;

        getter = new Get(Bytes.toBytes(key));

        //System.out.println("Querying HBase for: " + key);
        Result result = table.get(getter);
        //System.out.println("Result: " + result);

        byte[] valueBytes = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        String value = null;
        if (valueBytes != null) {
            value = Bytes.toString(valueBytes);
        }
        //System.out.println("Retrieved: " + value + ", Result.isempty = " + result.isEmpty());
        return value;
    }

    /**
     *
     * @param table
     * @param familyName
     * @param columnName
     * @param key
     * @param value
     * @throws IOException
     */
    private void put(Table table, String familyName, String columnName, String key, String value)
            throws IOException {

        //System.out.println("Putting '" + key + "' into HBase for value: " + value);
        Put putter = getPutRecord(familyName, columnName, key, value);
        table.put(putter);

    }
    
    private Put getPutRecord(String familyName, String columnName, String key, String value) {
    
        Put putter = null;
        putter = new Put(Bytes.toBytes(key));

        putter.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        
        return putter;
    }

    private long countResultSet(ResultScanner resultScanner) throws IOException {
        long count = 0;
        while (resultScanner.next() != null) {
            count++;
        }
        return count;
    }

    @Override
    public boolean windup() {

        // submit the last batch records to database if it is used batch insert
        if (batchCount > 0) {

            try {
                testTable.put(batchPuts);
                service.logMessage("Winding up: Completed last batch record put");
            } catch (IOException ex) {
                service.logMessage("SQLException - " + ex.getMessage());
                service.logMessage("ERROR - " + "Cannot submit the last batch for Hbase Batch Put.");
            }
        }
        
        if (testTable != null) {
            try {
                testTable.close();
                service.logMessage("Winding up: Closed test table - " + testDatabase + ":" + testTableName);
            } catch (IOException ex) {
                service.logMessage("SQLException - " + ex.getMessage());
                service.logMessage("ERROR - " + "Cannot close hbase table.");
            }
        }

        if (databaseConnection != null) {
            try {
                databaseConnection.close();
                service.logMessage("Winding up: HBase connection is closed - " + databaseConnection.toString());
                
                return true;
                
            } catch (IOException ex) {
                service.logMessage("SQLException - " + ex.getMessage());
                service.logMessage("ERROR - " + "Cannot close database connection.");
                return false;
            }
        }
        return true;
    }

}
