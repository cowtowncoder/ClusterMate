package com.fasterxml.clustermate.service.metrics;

import com.fasterxml.storemate.shared.TimeMaster;
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
     * Backend-dependant "raw" statistics
     */
    public BackendStats stats;

    /**
     * Timestamp of time when metrics were gathered
     */
    public long lastUpdated;

    public Boolean onlyFastStats;
    
    public String timeTaken;
    
    // for (de)serializer
    protected BackendMetrics() { }

    public BackendMetrics(long c, BackendStats rawStats)
    {
        Long l = rawStats.getCreationTime();
        lastUpdated = (l == null) ? 0L : l.longValue();
        count = c;
        stats = rawStats;
        onlyFastStats = rawStats.getOnlyFastStats();
        l = rawStats.getTimeTakenMsecs();
        timeTaken = (l == null)  ? "N/A" : TimeMaster.timeDesc(l.longValue());
    }
}
