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
import com.fasterxml.clustermate.service.metrics.ExternalOperationMetrics;
import com.fasterxml.clustermate.service.metrics.OperationMetrics;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;

@SuppressWarnings("serial")
public class SyncPullServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletWithMetricsBase
{
    protected final SyncHandler<K,E> _syncHandler;

    // may need JSON writer for errors:
    protected final ObjectWriter _jsonWriter;

    protected final OperationMetrics _pullMetrics;
    
    public SyncPullServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, null);
        _syncHandler = h;
        _jsonWriter = stuff.jsonWriter();
        final ServiceConfig serviceConfig = stuff.getServiceConfig();
        if (serviceConfig.metricsEnabled) {
            _pullMetrics = OperationMetrics.forListingOperation(serviceConfig, "syncPull");
        } else {
            _pullMetrics = null;
        }
    }

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics) {
        metrics.SYNCPULL = ExternalOperationMetrics.create(_pullMetrics);
    }
    
    @Override
    public void handlePost(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        final OperationMetrics metrics = _pullMetrics;
        Context timer = (metrics == null) ? null : metrics.start();
        try {
            _syncHandler.localPullEntries(request, response, request.getInputStream(), metadata);
            _addStdHeaders(response);
            response.writeOut(_jsonWriter);
        } finally {
            if (metrics != null) {
                metrics.finish(timer, metadata);
           }
        }
    }
}
