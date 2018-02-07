/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.hbasedbmsperftest;

import java.io.File;
import java.text.NumberFormat;
import java.util.Random;

/**
 *
 * @author glennm
 */
public class TestController extends Thread
            implements ControllerService {

    private Main mainFrame;
    private long reportingInterval = 10000000;
    private long reportingTimeInterval = 1000;
    
    private TestDriver driver = new TestDriverNoOp();

    private boolean abort = false;

    /**
     * Set the value of abort
     *
     * @param abort new value of abort
     */
    public void setAbort(boolean abort) {
        this.abort = abort;
    }

    /**
     * Get the value of mainFrame
     *
     * @return the value of mainFrame
     */
    public Main getMainFrame() {
        return mainFrame;
    }

    /**
     * Set the value of mainFrame
     *
     * @param mainFrame new value of mainFrame
     */
    public void setMainFrame(Main mainFrame) {
        this.mainFrame = mainFrame;
    }

    public long getReportingInterval() {
        return reportingInterval;
    }

    public void setReportingInterval(long reportingInterval) {
        this.reportingInterval = reportingInterval;
    }
    
    
    private void updateStatus(String text) {
        if (mainFrame != null) {
            mainFrame.updateStatusLabel(text);
        }
    }
    
    
    public void clearMetrics() {
        if (mainFrame != null) {
            mainFrame.clearMetrics();
        }
    }
    
    
    public void logMetric(PerformanceMetric metric) {
        if (mainFrame != null) {
            mainFrame.addMetric(metric);
        }
    }
    
    private void clearMessageLog() {
        if (mainFrame != null) {
            mainFrame.clearLogMessages();
        }
    }
    
    
    public void logMessage(String msg) {
        if (mainFrame != null) {
            mainFrame.addLogMessage(msg);
        }
    }

    public TestDriver getDriver() {
        return driver;
    }

    public void setDriver(TestDriver driver) {
        this.driver = driver;
    }
    
    
    private TestParameters testParameters = new TestParameters();

    /**
     * Get the value of testParameters
     *
     * @return the value of testParameters
     */
    public TestParameters getTestParameters() {
        return testParameters;
    }

    /**
     * Set the value of testParameters
     *
     * @param testParameters new value of testParameters
     */
    public void setTestParameters(TestParameters testParameters) {
        this.testParameters = testParameters;
    }

    
    private TestCase testCase = TestCase.GET;

    /**
     * Get the value of testCase
     *
     * @return the value of testCase
     */
    public TestCase getTestCase() {
        return testCase;
    }

    /**
     * Set the value of testCase
     *
     * @param testCase new value of testCase
     */
    public void setTestCase(TestCase testCase) {
        this.testCase = testCase;
    }


    @Override
    public void run() {
        super.run(); //To change body of generated methods, choose Tools | Templates.
        
        if (driver == null) {
            updateStatus("No Driver");
            return;
        }

        clearMessageLog();
        clearMetrics();
        
        intNumFmt = NumberFormat.getIntegerInstance();
        intNumFmt.setGroupingUsed(true);
        
        rateNumFmt = NumberFormat.getNumberInstance();
        rateNumFmt.setMaximumFractionDigits(2);
        rateNumFmt.setMinimumFractionDigits(2);
        
        Random rndFactory = new Random(System.currentTimeMillis());
        String stage = "Preparing environment";
        long rnd;
        long cnt = 0;
        long endTime = 0;
        try {
            updateStatus(mainFrame.STATUS_INIT);
            File propertiesFile = testParameters.getPropertiesFile();
            if (propertiesFile != null) {
                logMessage("Properties file: " + testParameters.getPropertiesFile().getAbsolutePath());
            } else {
                logMessage("Properties file: not specified");
            }
            stage = "Initialising Test Driver";
            logMessage("Initialising " + driver.getName());
            if (driver.initialise(this, testParameters)) {
                logMessage("Initialising complete.");
            } else {
                logMessage("Initialisation failed.");
                return;
            }
            
            stage = "Preparing Test Case";
            logMessage("Preparing test case: " + testCase);
            if (driver.prepareTestCase(testCase)) {
                logMessage("Preparation of test case complete.");
            } else {
                logMessage("Preparation of test case failed.");
                return;
            }
        
            updateStatus(mainFrame.STATUS_RUNNING);
            long loopLimit = testParameters.getScanCount();
            if (testCase == TestCase.PUT) {
                loopLimit = testParameters.getNumberOfIds();
            }
            
            stage = "Running Test Loop";
            startTime = System.currentTimeMillis();
            splitTimeBase = startTime;
            String key;
            long id = 0;
            boolean testOpStatus;
            for (cnt = 0; cnt < loopLimit; id++, cnt++) {
                chkUpdate(cnt);
                
                switch (testCase) {
                    case PUT:
                        id = cnt + testParameters.getStartId();
                        key = convertIdtoBase(id);
                        testOpStatus = driver.put (key, id);
                        break;
                    case SCAN:
                    case GET:
                        rnd = rndFactory.nextInt((int) testParameters.getNumberOfIds());
                        rnd += testParameters.getStartId();
                        key = convertIdtoBase(rnd);
                        if (testCase == TestCase.GET) {
                            testOpStatus = driver.get(key) != -1;
                        } else {
                            String scanKey = key.substring(0, key.length() - 1);
                            testOpStatus = driver.scan(scanKey) >= 0;
                        }
                        break;
                }
                
                if (abort) {
                    logMessage(formatProgress("Aborting at: ", startTime, cnt));
                    break;
                }
            }
            
            endTime = System.currentTimeMillis();
            
            if (abort) {
                updateStatus(mainFrame.STATUS_CANCELLED);
            } else {
                updateStatus(mainFrame.STATUS_COMPLETE);
            }
            
        } catch (Throwable t) {
            logMessage("Exception thrown: stage: '" + stage + "'. Exception: " + t);
            t.printStackTrace();
            updateStatus(mainFrame.STATUS_EXCEPTION);
        } finally {
            if (endTime == 0) {
                endTime = System.currentTimeMillis();
            }
            

            stage = "Winding up";
            logMessage("Winding up: " + driver.getName());
            if (driver.windup()) {
                logMessage("Windup successful");
            } else {
                logMessage("Windup failure");
            }
            
            
            logMessage("Total Load Time: " + ((endTime - startTime) / 1000.0) + "seconds.");
            String summaryMsg = formatProgress("Overall: ", (endTime - startTime), cnt);
            logMessage(summaryMsg);
            
            
            if (mainFrame != null) {
                mainFrame.notifyTestCompleted();
            }
        }
    }
    
    private long startTime = 0;
    private long splitTimeBase = 0;
    private long updateCntSinceLastUpdate = 0;
    
    private boolean chkUpdate(long cnt) {
        return chkUpdate(cnt, false);
    }
    
    
    /**
     * Check the update progress to see if a report is due.
     * The lastUpdate indicator should normally be false - unless it is the 
     * end of the test run.
     * @param cnt the number of records processed so far.
     * @param lastUpdate true if the test has reached its end.
     * @return true if an update was due.
     */
    private boolean chkUpdate(long cnt, boolean lastUpdate) {
        // TODO Check if a specified number of seconds have passed.
        // If either the number of records updated 
        updateCntSinceLastUpdate += 1;
        long now = System.currentTimeMillis();
        long splitTime = now - splitTimeBase;
        long elapsedTime = now - startTime;
        
        boolean reportUpdate = lastUpdate;
//        reportUpdate = ((updateCntSinceLastUpdate) % reportingInterval == 0);
        reportUpdate |= (splitTime > reportingTimeInterval);
        
        
        if (reportUpdate) {
//            System.out.println("Split: " + splitTime + "ms, " + updateCntSinceLastUpdate + " recs" + " cumul: " + elapsedTime + "ms, Cnt = " + cnt);
            PerformanceMetric metric = new PerformanceMetric(now, splitTime, updateCntSinceLastUpdate, elapsedTime, cnt);
            logMetric(metric);
            logMessage(formatProgress("Processed: ", elapsedTime, cnt));
            updateCntSinceLastUpdate = 0;
            splitTimeBase = now;
        }
        
        return reportUpdate;
    }
    
    private NumberFormat intNumFmt;
    private NumberFormat rateNumFmt;
    
    
    public String formatProgress(String prefix, long elapsedTime, long cnt) {
        StringBuilder msg = new StringBuilder(prefix);
        
        String numFmt = intNumFmt.format(cnt);
        updateStatus("processed: " + numFmt);
        msg.append(numFmt);
        
        long now = System.currentTimeMillis();
        double rate = ((double) cnt) / elapsedTime * 1000.0;
        String rateFmt = rateNumFmt.format(rate);
        
        msg.append(" records. Rate: ");
        msg.append(rateFmt);
        msg.append(" records / sec");
        
        // TODO: Add a "rate this interval" measure.
        System.out.println("Records processed: " + cnt + ", elapsed: " + (elapsedTime / 1000.0) + " seconds. Rate: " + rateFmt);

        String retMsg = msg.toString();

        return retMsg;
    }
    
    public static final long BASE = 95;
    public static final int WIDTH = 6;
    
    public String convertIdtoBase(long id) {
        long digitsNum [] = new long[10];
        
        long wrk = id;
        int ptr = 0;
        while (wrk > 0) {
            digitsNum[ptr++] = wrk % BASE;
            wrk = wrk / BASE;
        }
        
        StringBuilder sb = new StringBuilder();
        
        for (int i = WIDTH; i > ptr; i--) {
            sb.append(' ');
        }
        
        while (ptr > 0) {
            ptr--;
            
            char ch = (char) (digitsNum [ptr] + 32);
            sb.append(ch);
            
        }
        
//        System.out.println("convertIdToBase: " + id + " = '" + sb.toString() + "'");
        return sb.toString();
    }
    
}
