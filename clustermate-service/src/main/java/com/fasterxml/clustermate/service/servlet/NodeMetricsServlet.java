package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.backend.BackendStatsConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;

import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.http.StreamingEntityImpl;
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

    protected final LastAccessStore<?,?> _lastAccessStore;

    protected final TimeMaster _timeMaster;
    
    protected final AtomicReference<ExternalMetrics> _cachedMetrics
        = new AtomicReference<ExternalMetrics>();
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public NodeMetricsServlet(SharedServiceStuff stuff, Stores<?,?> stores)
    {
        // null -> use servlet path base as-is
        super(null, null);
        _timeMaster = stuff.getTimeMaster();
        _jsonWriter = stuff.jsonWriter(ExternalMetrics.class);
        _entryStore = stores.getEntryStore();
        _lastAccessStore = stores.getLastAccessStore();
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
        ExternalMetrics metrics = _cachedMetrics.get();
        long now = _timeMaster.currentTimeMillis();

        if (metrics == null || now >= (metrics.creationTime() + UPDATE_PERIOD_MSECS)) {
            metrics = _gatherMetrics(now);
            _cachedMetrics.set(metrics);
        }
        response = (ServletServiceResponse) response.ok(new StreamingEntityImpl(_jsonWriter, metrics))
                .setContentTypeJson();
        response.writeOut(null);
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
        
        return metrics;
    }
}
