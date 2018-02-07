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
    
    public String getName();
    
    public boolean initialise(ControllerService service, TestParameters testParameters);
    
    public boolean prepareTestCase(TestCase testCase);
    
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
    
    public boolean windup();

}
