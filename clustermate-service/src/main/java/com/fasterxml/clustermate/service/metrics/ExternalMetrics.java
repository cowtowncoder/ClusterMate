package com.fasterxml.clustermate.service.metrics;

/**
 * Simple POJO for serving metrics externally
 */
public class ExternalMetrics
{
    protected final long _creationTime;
    
    public BackendMetrics entryStore;

    public BackendMetrics entryIndex;

    public BackendMetrics lastAccessStore;

    public ExternalMetrics(long created) {
        _creationTime = created;
    }

    public long creationTime() { return _creationTime; }
}
