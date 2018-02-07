/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.hbasedbmsperftest;

import java.io.File;

/**
 *
 * @author glennm
 */
public class TestParameters {
    
        private long startId = 0;

    /**
     * Get the value of startId
     *
     * @return the value of startId
     */
    public long getStartId() {
        return startId;
    }

    /**
     * Set the value of startId
     *
     * @param startId new value of startId
     */
    public void setStartId(long startId) {
        this.startId = startId;
    }

        private long numberOfIds = 1000000;

    /**
     * Get the value of numberOfIds
     *
     * @return the value of numberOfIds
     */
    public long getNumberOfIds() {
        return numberOfIds;
    }

    /**
     * Set the value of numberOfIds
     *
     * @param numberOfIds new value of numberOfIds
     */
    public void setNumberOfIds(long numberOfIds) {
        this.numberOfIds = numberOfIds;
    }


    private File propertiesFile;

    /**
     * Get the value of propertiesFile
     *
     * @return the value of propertiesFile
     */
    public File getPropertiesFile() {
        return propertiesFile;
    }

    /**
     * Set the value of propertiesFile
     *
     * @param propertiesFile new value of propertiesFile
     */
    public void setPropertiesFile(File propertiesFile) {
        this.propertiesFile = propertiesFile;
    }
    
        private int scanCount = 10000;

    /**
     * Get the value of scanCount
     *
     * @return the value of scanCount
     */
    public int getScanCount() {
        return scanCount;
    }

    /**
     * Set the value of scanCount
     *
     * @param scanCount new value of scanCount
     */
    public void setScanCount(int scanCount) {
        this.scanCount = scanCount;
    }

    
        private boolean deleteAllDataFirst = false;

    /**
     * Get the value of deleteAllDataFirst
     *
     * @return the value of deleteAllDataFirst
     */
    public boolean isDeleteAllDataFirst() {
        return deleteAllDataFirst;
    }

    /**
     * Set the value of deleteAllDataFirst
     *
     * @param deleteAllDataFirst new value of deleteAllDataFirst
     */
    public void setDeleteAllDataFirst(boolean deleteAllDataFirst) {
        this.deleteAllDataFirst = deleteAllDataFirst;
    }

    
        private boolean useBatchOperations = false;

    /**
     * Get the value of useBatchOperations
     *
     * @return the value of useBatchOperations
     */
    public boolean isUseBatchOperations() {
        return useBatchOperations;
    }

    /**
     * Set the value of useBatchOperations
     *
     * @param useBatchOperations new value of useBatchOperations
     */
    public void setUseBatchOperations(boolean useBatchOperations) {
        this.useBatchOperations = useBatchOperations;
    }


}
