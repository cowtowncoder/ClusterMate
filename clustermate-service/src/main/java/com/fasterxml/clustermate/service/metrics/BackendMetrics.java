package com.fasterxml.clustermate.service.metrics;

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
    public Object stats;

    public BackendMetrics(long updateTime, long c, Object rawStats)
    {
        lastUpdated = updateTime;
        count = c;
        stats = rawStats;
    }
}
