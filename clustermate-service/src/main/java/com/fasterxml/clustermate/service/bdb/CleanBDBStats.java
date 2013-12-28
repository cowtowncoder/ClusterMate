package com.fasterxml.clustermate.service.bdb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.storemate.backend.bdbje.BDBBackendStats;
import com.sleepycat.je.EnvironmentStats;

/**
 * Helper class we only need for filtering out some unwanted
 * properties.
 */
public class CleanBDBStats // public for testing
    extends BDBBackendStats
{
    // this is an alternative to mix-ins, which would also work
    @JsonIgnoreProperties({
        "tips", "statGroups", "statGroupsMap" // names keep changing...
        /* 26-Sep-2013, tatu: Apparently these cause probs with 5.0.84:
         *   Curse: these keep on changing on version-by-version basis...
         */
        ,"avgBatchCacheMode", "avgBatchCritical", "avgBatchDaemon", "avgBatchEvictorThread", "avgBatchManual"
    })
    public EnvironmentStats getEnv() {
        return env;
    }

    public CleanBDBStats(BDBBackendStats raw)
    {
        super(raw);
        db = raw.db;
        env = raw.env;
    }
}