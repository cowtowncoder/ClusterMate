package com.fasterxml.clustermate.service.metrics;

/**
 * Simple POJO for serving metrics externally
 */
public class ExternalMetrics
{
    public StoreMetrics stores = new StoreMetrics();

    public AllOperationMetrics operations;

    public long lastUpdated;
    
    // for (de)serialization
    protected ExternalMetrics() { }
    public ExternalMetrics(long created) {
        lastUpdated = created;
    }

    public long creationTime() { return lastUpdated; }
}
