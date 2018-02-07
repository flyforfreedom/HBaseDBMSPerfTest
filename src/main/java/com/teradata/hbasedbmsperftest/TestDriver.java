/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.hbasedbmsperftest;

/**
 *
 * @author glennm
 */
public interface TestDriver {
    
    /**
     * Retrieve the name of this Data Service.
     * @return 
     */
    public String getName();
    
    /**
     * Initialise the connection(s) with the data server.
     * @param service A connection to the GUI to enable logging of progress messages.
     * @param testParameters The parameters entered by the user into the "TestSettings" dialog.
     * @return true if successful (false will abandon the test)
     */
    public boolean initialise(ControllerService service, TestParameters testParameters);
    
    /**
     * Make any preparations for the test case.
     * <p>
     * For example, if the user requested that the table be truncated before doing a load, then
     * this is where it should be done if it wasn't done during the initialise phase.
     * </p>
     * @param testCase a description of the test to be executed as defined by the TestCase enum (PUT, GET or Scan)
     * @return true if successful (false to abandon the test)
     */
    public boolean prepareTestCase(TestCase testCase);
    
    /**
     * Execute a put (or insert) operation for the given key and value.
     * @param key the key to put
     * @param value the value to put
     * @return true if successful (false will abandon the test)
     */
    public boolean put(String key, long value);
 
    /**
     * Get the record for the specified key.
     * @param key the key to look up.
     * @return the value (or -1 if not found).
     */
    public long get(String key);
    
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
    public long scan(String key);
    
    
    /**
     * Windup by closing any open sessions.
     * <p>
     * This method will always be called to support the closing of any open resources
     * such as database connections.
     * </p>
     * @return true if successful (false if unsuccessful)
     */
    public boolean windup();

}
