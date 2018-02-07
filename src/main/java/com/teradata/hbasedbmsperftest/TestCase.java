/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.hbasedbmsperftest;

import com.sun.org.apache.bcel.internal.generic.PUTFIELD;

/**
 *
 * @author glennm
 */
public enum TestCase {
    
    /** The test is a PUT test */
    PUT,
    /** The test is a GET test */
    GET,
    /** The test is a SCAN test */
    SCAN;
    
    
}
