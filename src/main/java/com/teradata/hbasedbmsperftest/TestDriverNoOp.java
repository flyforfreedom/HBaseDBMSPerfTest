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
public class TestDriverNoOp implements TestDriver {

    private ControllerService service;
    private long cnt = 0;
    
    @Override
    public boolean initialise(ControllerService service, TestParameters testParameters) {
        this.service = service;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            
        }
        cnt = 0;
        return true;
    }

    @Override
    public boolean prepareTestCase(TestCase testCase) {
        return true;
    }

    
    
    @Override
    public boolean put(String key, long value) {
        cnt++;
        
        if (cnt % 10000 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                
            }
        }
        return true;
    }

    @Override
    public boolean windup() {
        service.logMessage("NoOp Service winding up.");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            
        }
        return true;
    }

    @Override
    public long get(String key) {
        service.logMessage("Get: " + key);
        return 1;
    }

    @Override
    public long scan(String key) {
        service.logMessage("Get: " + key);
        return 1;
    }
    
    

    @Override
    public String getName() {
        return "No Op dummy driver";
    }
    
}
