package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import com.fasterxml.storemate.store.util.OperationDiagnostics;

import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.BackendMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalMetrics;
import com.sleepycat.je.EnvironmentStats;

/**
 * Stand-alone servlet that may be used for serving metrics information
 * regarding this node (and possibly its peers as well)
 */
@SuppressWarnings("serial")
public class NodeMetricsServlet extends ServletBase
{
    /**
     * Query parameter to request server to refresh its stats
     */
    protected final static String QUERY_PARAM_REFRESH = "refresh";

    /**
     * Query parameter to further request server to obtain full
     * statistics, not just cheap ones.
     */
    protected final static String QUERY_PARAM_FULL = "full";

    // Only probe for metrics once every 10 seconds (unless forced)
    protected final static long UPDATE_PERIOD_MSECS = 1L * 10 * 1000;

    // And even with forcing, do not query more often than once per second
    protected final static long MINIMUM_MSECS_BETWEEN_RECALC = 1000L;
    
    // let's only collect what can be gathered fast; not reset stats
    protected final static BackendStatsConfig BACKEND_STATS_CONFIG
        = BackendStatsConfig.DEFAULT
            .onlyCollectFast(true)
            .resetStatsAfterCollection(false);
    
    protected final ObjectWriter _jsonWriter;

    protected final StorableStore _entryStore;

    protected final AllOperationMetrics.Provider[] _metricsProviders;

    protected final LastAccessStore<?,?> _lastAccessStore;
    
    protected final TimeMaster _timeMaster;
    
    protected final AtomicReference<SerializedMetrics> _cachedMetrics
        = new AtomicReference<SerializedMetrics>();
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public NodeMetricsServlet(SharedServiceStuff stuff, Stores<?,?> stores,
            AllOperationMetrics.Provider[] metricsProviders)
    {
        // null -> use servlet path base as-is
        super(stuff, null, null);
        _timeMaster = stuff.getTimeMaster();
        // for robustness, allow empty beans...

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
        _entryStore = stores.getEntryStore();
        _lastAccessStore = stores.getLastAccessStore();
        _metricsProviders = metricsProviders;
    }

    /*
    /**********************************************************************
    /* End points: for now just GET
    /**********************************************************************
     */
    
    /**
     * GET is used for simple access of cluster status
     */
    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        try {
            // One more thing: is caller trying to force refresh?
            boolean forceRefresh = "true".equals(request.getQueryParameter(QUERY_PARAM_REFRESH));
            boolean full = "true".equals(request.getQueryParameter(QUERY_PARAM_FULL));

            SerializedMetrics ser = _cachedMetrics.get();
            long now = _timeMaster.currentTimeMillis();
    
            if (_shouldRefresh(forceRefresh, now, ser)) {
                ExternalMetrics metrics = _gatherMetrics(now, full);
                byte[] raw;
                    raw = _jsonWriter
                            // for diagnostics:
                            .withDefaultPrettyPrinter()
                            .writeValueAsBytes(metrics);
                ser = new SerializedMetrics(now, raw);
                _cachedMetrics.set(ser);
            }
            response = (ServletServiceResponse) response.ok()
                    .setContentTypeJson();
            response.writeRaw(ser.serialized);
        } catch (Exception e) {
            String msg = "Failed to serialize Metrics: "+e;
            LOG.warn(msg, e);
            response = (ServletServiceResponse) response
                .internalError(msg)
                .setContentTypeText()
                ;
            response.writeOut(_jsonWriter);
        }
    }

    private static boolean _shouldRefresh(boolean forced, long now, SerializedMetrics metrics)
    {
        if (metrics == null) {
            return true;
        }
        long wait = forced ? MINIMUM_MSECS_BETWEEN_RECALC : UPDATE_PERIOD_MSECS;
        return now >= (metrics.created + wait);
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
        BackendStatsConfig conf = BACKEND_STATS_CONFIG;
        if (fullStats) {
            conf = conf.onlyCollectFast(false);
        }
        metrics.stores.entries = new BackendMetrics(creationTime,
                entries.getEntryCount(),
                _clean(entries.getEntryStatistics(BACKEND_STATS_CONFIG)));
        metrics.stores.entryIndex = new BackendMetrics(creationTime,
                entries.getIndexedCount(),
                _clean(entries.getIndexStatistics(BACKEND_STATS_CONFIG)));
        metrics.stores.lastAccessStore = new BackendMetrics(creationTime,
                _lastAccessStore.getEntryCount(),
                _clean(_lastAccessStore.getEntryStatistics(BACKEND_STATS_CONFIG)));

        AllOperationMetrics opMetrics = new AllOperationMetrics();
        metrics.operations = opMetrics;
        for (AllOperationMetrics.Provider provider : _metricsProviders) {
            provider.fillOperationMetrics(opMetrics);
        }
        return metrics;
    }

    protected BackendStats _clean(BackendStats stats)
    {
        // Nasty back-dep, but has to do for now...
        if (stats instanceof BDBBackendStats) {
            return new CleanBDBStats((BDBBackendStats) stats);
        }
        return stats;
    }

    /*
    /**********************************************************************
    /* Helper class(es)
    /**********************************************************************
     */

    /**
     * Helper class we only need for filtering out some unwanted
     * properties.
     */
    static class CleanBDBStats
        extends BDBBackendStats
    {
        // this is an alternative to mix-ins, which would also work
        @JsonIgnoreProperties({ "tips", "statGroups" })
        public EnvironmentStats getEnv() {
            return env;
        }

        public CleanBDBStats(BDBBackendStats raw)
        {
            super();
            db = raw.db;
            env = raw.env;
        }
    }

    private final static class SerializedMetrics
    {
        public final long created;
        public final byte[] serialized;

        public SerializedMetrics(long cr, byte[] data) {
            created = cr;
            serialized = data;
        }
    }
}
