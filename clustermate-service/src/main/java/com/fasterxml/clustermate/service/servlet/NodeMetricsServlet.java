package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.backend.BackendStatsConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;

import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.BackendMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalMetrics;

/**
 * Stand-alone servlet that may be used for serving metrics information
 * regarding this node (and possibly its peers as well)
 */
@SuppressWarnings("serial")
public class NodeMetricsServlet extends ServletBase
{
    // Only probe for metrics once every 5 minutes
    protected final static long UPDATE_PERIOD_MSECS = 5L * 60 * 1000;
    
    // let's only collect what can be gathered fast; not reset stats
    protected final static BackendStatsConfig BACKEND_STATS_CONFIG
        = BackendStatsConfig.DEFAULT
            .onlyCollectFast(true)
            .resetStatsAfterCollection(false);
    
    protected final ObjectWriter _jsonWriter;

    protected final StorableStore _entryStore;

    protected final AllOperationMetrics.Provider _operationMetrics;

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
            AllOperationMetrics.Provider opMetrics)
    {
        // null -> use servlet path base as-is
        super(null, null);
        _timeMaster = stuff.getTimeMaster();
        // for robustness, allow empty beans...
        _jsonWriter = stuff.jsonWriter(ExternalMetrics.class)
                .without(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        _entryStore = stores.getEntryStore();
        _lastAccessStore = stores.getLastAccessStore();
        _operationMetrics = opMetrics;
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
            SerializedMetrics ser = _cachedMetrics.get();
            long now = _timeMaster.currentTimeMillis();
    
            if (ser == null || now >= ser.cacheUntil) {
                ExternalMetrics metrics = _gatherMetrics(now);
                byte[] raw;
                    raw = _jsonWriter
                            // for diagnostics:
                            .withDefaultPrettyPrinter()
                            .writeValueAsBytes(metrics);
                ser = new SerializedMetrics(now + UPDATE_PERIOD_MSECS, raw);
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

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected ExternalMetrics _gatherMetrics(long creationTime)
    {
        ExternalMetrics metrics = new ExternalMetrics(creationTime);
        StoreBackend entries = _entryStore.getBackend();
        metrics.entryStore = new BackendMetrics(creationTime,
                entries.getEntryCount(),
                entries.getEntryStatistics(BACKEND_STATS_CONFIG));
        metrics.entryIndex = new BackendMetrics(creationTime,
                entries.getIndexedCount(),
                entries.getIndexStatistics(BACKEND_STATS_CONFIG));
        metrics.lastAccessStore = new BackendMetrics(creationTime,
                _lastAccessStore.getEntryCount(),
                _lastAccessStore.getEntryStatistics(BACKEND_STATS_CONFIG));

        metrics.operations = _operationMetrics.getOperationMetrics();
        
        return metrics;
    }

    /*
    /**********************************************************************
    /* Helper class(es)
    /**********************************************************************
     */

    private final static class SerializedMetrics
    {
        public final long cacheUntil;
        public final byte[] serialized;

        public SerializedMetrics(long expiration, byte[] data) {
            cacheUntil = expiration;
            serialized = data;
        }
    }
}
