package com.fasterxml.clustermate.service.metrics;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
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
    public String type;
    
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
    protected Map<String,Object> extraStats;
    
    // for (de)serializer
    protected BackendMetrics() { }

    public BackendMetrics(long c, BackendStats rawStats)
    {
        this(c, rawStats, rawStats.extraStats());
    }
    
    public BackendMetrics(long c, BackendStats rawStats, Map<String,Object> extraStats)
    {
        Long l = rawStats.getCreationTime();
        type = rawStats.getType();
        lastUpdated = (l == null) ? 0L : l.longValue();
        count = c;
        this.extraStats = extraStats;
        onlyFastStats = rawStats.getOnlyFastStats();
        l = rawStats.getTimeTakenMsecs();
        timeTaken = (l == null)  ? "N/A" : TimeMaster.timeDesc(l.longValue());
    }

    @JsonAnyGetter
    public Map<String,Object> backendDependantStats() {
        return extraStats;
    }
}
