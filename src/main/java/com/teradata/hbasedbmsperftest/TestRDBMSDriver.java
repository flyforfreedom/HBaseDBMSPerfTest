/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.hbasedbmsperftest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author md186047
 */
public class TestRDBMSDriver implements TestDriver {

    private Connection databaseConnection; 
    private ControllerService service;
    private TestParameters testParameters;
    private TestCase testCase;
   
    private final String insertTable = "hbasePerfTest";
    private final String scanTable = "hbasePerfTest";
    private final String getTable = "hbasePerfTest";
    
    private PreparedStatement putPreparedStmt;
    private int batchCount = 0 ;
    private final static int BATCH_SIZE = 100;
    
    private PreparedStatement getPreparedStmt;
    private PreparedStatement scanPreparedStmt;
    
    private final static String PROPERTIES_URL = "url";
    private final static String PROPERTIES_USERID = "userId";
    private final static String PROPERTIES_PASSWORD = "password";
    
    public TestRDBMSDriver() {
    }

    @Override
    public String getName() {
        return "RDBMS - Teradata";
    }

    @Override
    public boolean initialise(ControllerService service, TestParameters testParameters) {
        
        this.service = service;
        this.testParameters = testParameters;
                      
        Properties properties = new Properties();
        File propertiesFile = testParameters.getPropertiesFile();
	InputStream fileInput = null;
        
	try {
            
            fileInput = new FileInputStream(propertiesFile);
            // load a properties file
            properties.load(fileInput);
            
           service.logMessage("propertiesFile absolute path is: " + propertiesFile.getAbsolutePath());


            // get the property value and print it out
            String connectionUrl = properties.getProperty(PROPERTIES_URL);
            String databaseUserId = properties.getProperty(PROPERTIES_USERID);
            String databasePassword = properties.getProperty(PROPERTIES_PASSWORD);

            // establish datbase connection
            try {
                Class.forName("com.teradata.jdbc.TeraDriver");
                
                this.databaseConnection = DriverManager.getConnection(connectionUrl, databaseUserId, databasePassword);
                service.logMessage("Database connection is established - " + databaseConnection.toString());
                               
                return true;
                
            } catch (SQLException ex) {
                service.logMessage("SQLException - " + ex.getMessage());
                service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot establish database connection for " + connectionUrl);
            } catch (ClassNotFoundException ex) {
                service.logMessage("ClassNotFoundException - " + ex.getMessage());
                service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot find jdbc driver");
            } 
            
	} catch (FileNotFoundException e) {
            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Properties file does NOT exist " + propertiesFile.getAbsolutePath());
	} catch (IOException e) {
            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot read file " + propertiesFile.getAbsolutePath());
        } finally {
            if (fileInput != null) {
                try {
                    fileInput.close();
                } catch (IOException e) {
                    service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Properties File " + propertiesFile + "is null");
                }
            }
        }
        return false;
        
    }
    
    @Override
    public boolean prepareTestCase(TestCase testCase) {
        this.testCase = testCase;

        switch (testCase) {
            case PUT:
                service.logMessage("Test case is PUT.");

                PreparedStatement deleteStmt = null;
                try {
                    
                    if (testParameters.isDeleteAllDataFirst()) {
                        //cleanup table
                        service.logMessage("Clean up table - " + insertTable);
                        String cleanupTable = "Delete from " + insertTable + ";";
                        deleteStmt = databaseConnection.prepareStatement(cleanupTable);
                        deleteStmt.executeUpdate();
                    }
                    
                    // prepare put statement
                    batchCount = 0;
                    String insertQuery = "INSERT INTO " + insertTable + " values (?, ?);";
                    putPreparedStmt = databaseConnection.prepareStatement(insertQuery);

                    return true;
                } catch (SQLException ex) {
                    service.logMessage("SQLException - " + ex.getMessage());
                    service.logMessage("Error - Cannot prepare insert query for database connenction.");
                    return false;
                } finally {
                    if (deleteStmt != null) {
                        try {
                            deleteStmt.close();
                        } catch (SQLException ex) {
                            service.logMessage("SQLExpception - " + ex.getMessage());
                            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot close database scan statement.");
                        }
                    }
                }

            case GET:
                service.logMessage("Test case is GET.");

                try {
                    String selectQuery = "SELECT val FROM " + getTable + " WHERE id = ?;";
                    getPreparedStmt = databaseConnection.prepareStatement(selectQuery);
                    return true;

                } catch (SQLException ex) {
                    service.logMessage("SQLException - " + ex.getMessage());
                    service.logMessage("Error - Cannot prepare GET query for database connenction.");
                    return false;
                }

            case SCAN:
                service.logMessage("Test case is SCAN.");
                
                try {
                    String selectQuery = "SELECT val FROM " + scanTable + " WHERE id like ?;";
                    scanPreparedStmt = databaseConnection.prepareStatement(selectQuery);
                    return true;

                } catch (SQLException ex) {
                    service.logMessage("SQLException - " + ex.getMessage());
                    service.logMessage("Error - Cannot prepare GET query for database connenction.");
                    return false;
                }
                
            default:
                service.logMessage("Haven't define a test case.");
                return true;

        }
    }

    @Override
    public boolean put(String key, long value) {

        //return putLiteralSql(key, value);
        if (testParameters.isUseBatchOperations()) {
            return putBatch(key, value);
        }
        return putPrepared(key, value);

    }

    /**
     * Low performance as it calls prepareStatement() for each value
     * @param key
     * @param value
     * @return 
     */
    private boolean putLiteralSql(String key, long value) {
        PreparedStatement stmt = null;
        String query = null;

        // escape single quote in Teradata
        if (key.contains("'")) {
            key = key.replaceAll("'", "''");
        }

        try {
            // build insert string like - insert into hbaseperftest values ('aa', 1234);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("insert into ");
            stringBuilder.append(insertTable);
            stringBuilder.append(" values ( ");
            stringBuilder.append("'");
            stringBuilder.append(key);
            stringBuilder.append("'");
            stringBuilder.append(",");
            stringBuilder.append(value);
            stringBuilder.append(") ;");

            query = stringBuilder.toString();
            //service.logMessage("Insert query is: " + query);
            stmt = databaseConnection.prepareStatement(query);
            stmt.executeUpdate();

            return true;

        } catch (SQLException ex) {
            service.logMessage("SQLExpception - " + ex.getMessage());
            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot execuate database query - " + query);
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    service.logMessage("SQLExpception - " + ex.getMessage());
                    service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Database query statement is null");
                }
            }
        }
    }
    
    /**
     * 
     * @param key
     * @param value
     * @return 
     */
    private boolean putPrepared(String key, long value) {
    
        try {
            putPreparedStmt.setString(1, key);
            putPreparedStmt.setLong(2, value);
            //service.logMessage("Insert query is: " + putPreparedStmt);
            
            putPreparedStmt.executeUpdate();
             
            return true;
        } catch (SQLException ex) {
            service.logMessage("SQLExpception - " + ex.getMessage());
            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot execuate database insert query - " + putPreparedStmt);
            return false;
        }
    }
    
    
    /**
     * Batch insert per 100 records
     * @param key
     * @param value
     * @return 
    */  
    private boolean putBatch(String key, long value){
        
        try {
            putPreparedStmt.setString(1, key);
            putPreparedStmt.setLong(2, value);
            //service.logMessage("Insert query is: " + putPreparedStmt);
            
            putPreparedStmt.addBatch();
            batchCount++; 
            
            if (batchCount > BATCH_SIZE) {
                putPreparedStmt.executeBatch();
                batchCount = 0 ; 
            }
                        
            return true;
        } catch (SQLException ex) {
            service.logMessage("SQLExpception - " + ex.getMessage());
            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot execuate database insert statement - " + putPreparedStmt);
            return false;
        } 
    }
    
    /**
     * Get the record for the specified key.
     * @param key the key to look up.
     * @return the value (or -1 if not found).
     */
    @Override
    public long get(String key) {
        
        try {
            getPreparedStmt.setString(1, key);
            ResultSet result =  getPreparedStmt.executeQuery();
            if (result.next()) {
                return result.getLong(1);
            } else {
                return -1;
            }
           
        } catch (SQLException ex) {
            service.logMessage("SQLExpception - " + ex.getMessage());
            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot execuate database select statement - " + getPreparedStmt);
            
            return -1;
        }
    }

    /**
     * Scan the database for the specified partial partial key.
     * The characters specified are the matching character and will be matched to the
     * beginning of the key.
     * 
     * In RDDMS terms, the scan is:
     *     where key like supplied_key_value || '%'
     * @param key
     * @return the count of records matched.
     */
    @Override
    public long scan(String key) {
     
        String likeValue = key + "%";
        ResultSet resultset = null;

        try {
            scanPreparedStmt.setString(1, likeValue);
            resultset = scanPreparedStmt.executeQuery();
            
            long rowCount = countResultSet(resultset);
            //service.logMessage("RDBMS Scan completed. Row count is: " + rowCount);
            return rowCount;

        } catch (SQLException ex) {
            service.logMessage("SQLExpception - " + ex.getMessage());
            service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot execuate database select statement - " + scanPreparedStmt);

            return -1;
        } finally {
            if (resultset != null) {
                try {
                    resultset.close();
                } catch (SQLException ex) {
                    service.logMessage("SQLExpception - " + ex.getMessage());
                    service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot close resultset for scan");
                }
            }
        }
    }
    
    private long countResultSet(ResultSet resultset) throws SQLException {
        long rsCount = 0;
        while (resultset.next()) {
            rsCount++;
        }
        return rsCount;
    }
    
    

    @Override
    public boolean windup() {

        // submit the last batch records to database if it is used batch insert
        if (batchCount > 0 ) {
            try {
                putPreparedStmt.executeBatch();
                service.logMessage("Winding up: Completed last batch record insert");
            } catch (SQLException ex) {
                service.logMessage("SQLExpception - " + ex.getMessage());
                service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot submit the last batch for database insert query - " + putPreparedStmt);;
            }
        }

        if (putPreparedStmt != null) {
            try {
                putPreparedStmt.close();
                service.logMessage("Winding up: Closed SQL put prepared statement");
            } catch (SQLException ex) {
                service.logMessage("SQLExpception - " + ex.getMessage());
                service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot close database insert statement.");
            }
        }
        
        if (getPreparedStmt != null) {
            try {
                getPreparedStmt.close();
                service.logMessage("Winding up: Closed SQL get prepared statement");
            } catch (SQLException ex) {
                service.logMessage("SQLExpception - " + ex.getMessage());
                service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot close database insert statement.");
            }
        }
        
        if (scanPreparedStmt != null) {
            try {
                scanPreparedStmt.close();
                service.logMessage("Winding up: Closed SQL scan prepared statement");
            } catch (SQLException ex) {
                service.logMessage("SQLExpception - " + ex.getMessage());
                service.logMessage("ERROR - " + TestRDBMSDriver.class.getName() + ":" + "Cannot close database scan statement.");
            }
        }
            
        if (databaseConnection != null) {
            try {
                databaseConnection.close();
                service.logMessage("Winding up: Database connection is closed - " + databaseConnection.toString());
                
                return true;
                
            } catch (SQLException ex) {
                service.logMessage("SQLException - " + ex.getMessage());
                service.logMessage("ERROR - " + "Cannot close database connection.");
                return false;
            }
        }
        
        return true;
    }
        

   
}
