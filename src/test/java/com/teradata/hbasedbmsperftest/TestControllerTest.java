/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.hbasedbmsperftest;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author glennm
 */
public class TestControllerTest {
    
    public TestControllerTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getMainFrame method, of class TestController.
     */
    @Test
    public void testGetMainFrame() {
        
    }

    /**
     * Test of setMainFrame method, of class TestController.
     */
    @Test
    public void testSetMainFrame() {
        
    }
    
    /**
     * Test of getTestParameters method, of class TestController.
     */
    @Test
    public void testGetTestParameters() {

    }

    /**
     * Test of setTestParameters method, of class TestController.
     */
    @Test
    public void testSetTestParameters() {

    }

    /**
     * Test of run method, of class TestController.
     */
    @Test
    public void testRun() {

    }

    /**
     * Test of convertIdtoBase method, of class TestController.
     */
    @Test
    public void testConvertIdtoBase() {
        System.out.println("convertIdtoBase");
        long id = 0L;
        TestController instance = new TestController();
        
        for (int i = 0; i < 100; i++) {
            System.out.println(i + ": " + instance.convertIdtoBase(i));
        }
        

        String expResult = "      ";
        String result = instance.convertIdtoBase(id);
        System.out.println("id = " + id + ", result = '" + result + "'");
        assertEquals(expResult, result);
        
        
        expResult = "     !";
        id = 1;
        result = instance.convertIdtoBase(id);
        System.out.println("id = " + id + ", result = '" + result + "'");
        assertEquals(expResult, result);
        
        expResult = "     H";
        id = 40;
        result = instance.convertIdtoBase(id);
        System.out.println("id = " + id + ", result = '" + result + "'");
        assertEquals(expResult, result);
        
        expResult = "     ~";
        id = 94;
        result = instance.convertIdtoBase(id);
        System.out.println("id = " + id + ", result = '" + result + "'");
        assertEquals(expResult, result);
        
        expResult = "    ! ";
        id = 95;
        result = instance.convertIdtoBase(id);
        System.out.println("id = " + id + ", result = '" + result + "'");
        assertEquals(expResult, result);
        
        expResult = "    !%";
        id = 100;
        result = instance.convertIdtoBase(id);
        System.out.println("id = " + id + ", result = '" + result + "'");
        assertEquals(expResult, result);
    }
    
}
