package com.fasterxml.clustermate.service.metrics;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.storemate.store.backend.BackendStats;

/**
 * Helper class we only need for filtering out some unwanted
 * properties from BDB-backend statistics. Note that although
 * it is unfortunate to have such backend-specific stuff, there
 * is no hard-dependency to BDB lib; and this is required because
 * StoreMate does not have JSON dependency which we need for filtering.
 */
public class CleanedBDBBackendMetrics extends BackendMetrics
{
    private final Object env;
    
    // this is an alternative to mix-ins, which would also work
    @JsonIgnoreProperties({
        "tips", "statGroups", "statGroupsMap" // names keep changing...
        /* 26-Sep-2013, tatu: Apparently these cause probs with 5.0.84:
         *   Curse: these keep on changing on version-by-version basis...
         */
        ,"avgBatchCacheMode", "avgBatchCritical", "avgBatchDaemon", "avgBatchEvictorThread", "avgBatchManual"
    })
    public Object getEnv() {
        return env;
    }
    
    private CleanedBDBBackendMetrics(long count, BackendStats rawStats, Map<String,Object> stats,
            Object env) {
        super(count, rawStats, stats);
        this.env = env;
    }

    public static CleanedBDBBackendMetrics construct(long count, BackendStats raw)
    {
        Map<String,Object> stats = raw.extraStats();
        Object env = stats.remove("env");
        return new CleanedBDBBackendMetrics(count, raw, stats, env);
    }
}