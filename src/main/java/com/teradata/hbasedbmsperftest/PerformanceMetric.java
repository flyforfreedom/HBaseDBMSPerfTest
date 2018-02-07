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
public class PerformanceMetric {

    public PerformanceMetric(long timestamp, long splitTime, long splitRecordCount, long elapsedTime, long cumulRecordCount) {
        this.timestamp = timestamp;
        this.splitTime = splitTime;
        this.splitRecordCount = splitRecordCount;
        this.elapsedTime = elapsedTime;
        this.cumulRecordCount = cumulRecordCount;
    }

    public PerformanceMetric() {
    }
    
    
        private long timestamp;

    /**
     * Get the value of timestamp
     *
     * @return the value of timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Set the value of timestamp
     *
     * @param timestamp new value of timestamp
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    
    private long splitTime;

    /**
     * Get the value of splitTime
     *
     * @return the value of splitTime
     */
    public long getSplitTime() {
        return splitTime;
    }

    /**
     * Set the value of splitTime
     *
     * @param splitTime new value of splitTime
     */
    public void setSplitTime(long splitTime) {
        this.splitTime = splitTime;
    }

    private long splitRecordCount;

    /**
     * Get the value of splitRecordCount
     *
     * @return the value of splitRecordCount
     */
    public long getSplitRecordCount() {
        return splitRecordCount;
    }

    /**
     * Set the value of splitRecordCount
     *
     * @param splitRecordCount new value of splitRecordCount
     */
    public void setSplitRecordCount(long splitRecordCount) {
        this.splitRecordCount = splitRecordCount;
    }

    private long elapsedTime;

    /**
     * Get the value of elapsedTime
     *
     * @return the value of elapsedTime
     */
    public long getElapsedTime() {
        return elapsedTime;
    }

    /**
     * Set the value of elapsedTime
     *
     * @param elapsedTime new value of elapsedTime
     */
    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    private long cumulRecordCount;

    /**
     * Get the value of cumulRecordCount
     *
     * @return the value of cumulRecordCount
     */
    public long getCumulRecordCount() {
        return cumulRecordCount;
    }

    /**
     * Set the value of cumulRecordCount
     *
     * @param cumulRecordCount new value of cumulRecordCount
     */
    public void setCumulRecordCount(long cumulRecordCount) {
        this.cumulRecordCount = cumulRecordCount;
    }

    @Override
    public String toString() {
        return "PerformanceMetric{" + "timestamp=" + timestamp + ", splitTime=" + splitTime + ", splitRecordCount=" + splitRecordCount + ", elapsedTime=" + elapsedTime + ", cumulRecordCount=" + cumulRecordCount + '}';
    }

    
    
    
}
