package com.fasterxml.clustermate.service.metrics;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.fasterxml.storemate.backend.bdbje.BDBBackendStats;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.backend.BackendStats;
import com.fasterxml.storemate.store.backend.BackendStatsConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.bdb.CleanBDBStats;

/**
 * Helper class that is used to access metrics from a separate thread,
 * so that we can avoid both congestion by concurrent metrics
 * refreshes, and to allow for timeouts.
 */
public class BackgroundMetricsAccessor
{
    // Only probe for metrics once every 10 seconds (unless forced)
    protected final static long UPDATE_PERIOD_MSECS = 1L * 10 * 1000;

    // And even with forcing, do not query more often than once per second
    protected final static long MINIMUM_MSECS_BETWEEN_RECALC = 1000L;

    // let's only collect what can be gathered fast; not reset stats
    protected final static BackendStatsConfig BACKEND_STATS_CONFIG
        = BackendStatsConfig.DEFAULT
            .onlyCollectFast(true)
            .resetStatsAfterCollection(false);
    
    protected final AtomicReference<SerializedMetrics> _cachedMetrics
        = new AtomicReference<SerializedMetrics>();

    protected final ObjectWriter _jsonWriter;

    protected final TimeMaster _timeMaster;

    protected final StorableStore _entryStore;

    protected final AllOperationMetrics.Provider[] _metricsProviders;

    protected final LastAccessStore<?,?,?> _lastAccessStore;
    
    public BackgroundMetricsAccessor(SharedServiceStuff stuff, Stores<?,?> stores,
            AllOperationMetrics.Provider[] metricsProviders)
    {
        _timeMaster = stuff.getTimeMaster();

        _entryStore = stores.getEntryStore();
        _lastAccessStore = stores.getLastAccessStore();
        _metricsProviders = metricsProviders;

        /* 16-May-2013, tatu: Need to use separate mapper just because
         *   our default inclusion mechanism may differ...
         */
        ObjectMapper mapper = new ObjectMapper();
        /* Looks like NON_DEFAULT can not be used, just because not all
         * classes can be instantiated. So for now need to use NON_EMPTY...
         */
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        _jsonWriter = mapper.writerWithType(ExternalMetrics.class)
                .without(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }
    
    public SerializedMetrics getMetrics(boolean forceRefresh, boolean full)
        throws IOException
    {
        SerializedMetrics ser = _cachedMetrics.get();
        final long now = _timeMaster.currentTimeMillis();
        if (_shouldRefresh(forceRefresh, now, ser)) {
            ExternalMetrics metrics = _gatherMetrics(now, full);
            ser = new SerializedMetrics(_jsonWriter.getFactory(), now,
                    _jsonWriter.writeValueAsBytes(metrics));
            _cachedMetrics.set(ser);
        }
        return ser;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected ExternalMetrics _gatherMetrics(long creationTime, boolean fullStats)
    {
        ExternalMetrics metrics = new ExternalMetrics(creationTime);
        StoreBackend entries = _entryStore.getBackend();
        BackendStatsConfig conf = BACKEND_STATS_CONFIG
                .onlyCollectFast(!fullStats);
        metrics.stores.entries = new BackendMetrics(entries.getEntryCount(),
                _clean(entries.getEntryStatistics(conf)));
        metrics.stores.entryIndex = new BackendMetrics(entries.getIndexedCount(),
                _clean(entries.getIndexStatistics(conf)));
        metrics.stores.lastAccessStore = new BackendMetrics(_lastAccessStore.getEntryCount(),
                _clean(_lastAccessStore.getEntryStatistics(conf)));

        AllOperationMetrics opMetrics = new AllOperationMetrics();
        metrics.operations = opMetrics;
        for (AllOperationMetrics.Provider provider : _metricsProviders) {
            provider.fillOperationMetrics(opMetrics);
        }
        return metrics;
    }

    private static boolean _shouldRefresh(boolean forced, long now, SerializedMetrics metrics)
    {
        if (metrics == null) {
            return true;
        }
        long wait = forced ? MINIMUM_MSECS_BETWEEN_RECALC : UPDATE_PERIOD_MSECS;
        return now >= (metrics.created + wait);
    }

    protected BackendStats _clean(BackendStats stats)
    {
        // Nasty back-dep, but has to do for now...
        if (stats instanceof BDBBackendStats) {
            return new CleanBDBStats((BDBBackendStats) stats);
        }
        return stats;
    }
}
