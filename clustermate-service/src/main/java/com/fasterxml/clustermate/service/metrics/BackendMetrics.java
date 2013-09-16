package com.fasterxml.clustermate.service.metrics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.backend.BackendStats;

/**
 * POJO for simple backend metrics
 */
@JsonPropertyOrder({
    // define ordering just for readability for manual debug cases
    "count", "onlyFastStats", "timeTaken", "lastUpdated", "stats" })
public class BackendMetrics
{
    /**
     * Approximate count as reported by underlying store
     */
    public long count;

    public Boolean onlyFastStats;

    /**
     * How long did it take to generate these metrics?
     */
    public String timeTaken;

    /**
     * Timestamp of time when metrics were gathered
     */
    public long lastUpdated;

    /**
     * Backend-dependant "raw" statistics
     */
    @JsonIgnoreProperties({ // since these would otherwise be duplicated
        "creationTime", "timeTakenMsecs", "onlyFastStats"
    })
    public BackendStats stats;
    
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
