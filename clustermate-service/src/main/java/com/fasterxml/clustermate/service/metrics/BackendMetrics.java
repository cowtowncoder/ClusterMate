package com.fasterxml.clustermate.service.metrics;

import com.fasterxml.storemate.store.backend.BackendStats;

/**
 * POJO for simple backend metrics
 */
public class BackendMetrics
{
    /**
     * Approximate count as reported by underlying store
     */
    public long count;

    /**
     * Timestamp of time when metrics were gathered
     */
    public long lastUpdated;

    /**
     * Backend-dependant "raw" statistics
     */
    public BackendStats stats;

    // for (de)serializer
    protected BackendMetrics() { }

    public BackendMetrics(long updateTime, long c, BackendStats rawStats)
    {
        lastUpdated = updateTime;
        count = c;
        stats = rawStats;
    }
}
