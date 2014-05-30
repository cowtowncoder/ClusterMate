package com.fasterxml.clustermate.servlet;

import java.io.IOException;

import com.codahale.metrics.Timer.Context;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.OperationMetrics;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;

abstract class SyncPullServletBase<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletWithMetricsBase
{
    private static final long serialVersionUID = 1L;

    protected final SyncHandler<K,E> _syncHandler;

    // may need JSON writer for errors:
    protected final ObjectWriter _jsonWriter;

    protected final OperationMetrics _pullMetrics;
    
    public SyncPullServletBase(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h, String metricsName)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, null);
        _syncHandler = h;
        _jsonWriter = stuff.jsonWriter();
        final ServiceConfig serviceConfig = stuff.getServiceConfig();
        if (serviceConfig.metricsEnabled) {
            _pullMetrics = OperationMetrics.forListingOperation(serviceConfig, metricsName);
        } else {
            _pullMetrics = null;
        }
    }

    @Override
    public abstract void fillOperationMetrics(AllOperationMetrics metrics);

    protected abstract ServletServiceResponse _pullEntries(ServletServiceRequest request,
            ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException;

    @Override
    public void handlePost(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        final OperationMetrics metrics = _pullMetrics;
        Context timer = (metrics == null) ? null : metrics.start();
        try {
            response = _pullEntries(request, response, metadata);
            _addStdHeaders(response);
            response.writeOut(_jsonWriter);
        } finally {
            if (metrics != null) {
                metrics.finish(timer, metadata);
           }
        }
    }
}
