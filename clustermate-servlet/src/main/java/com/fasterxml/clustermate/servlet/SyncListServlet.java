package com.fasterxml.clustermate.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.Timer.Context;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

import com.fasterxml.clustermate.api.ClusterMateConstants;
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
public class SyncListServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletWithMetricsBase
{
    protected final SyncHandler<K,E> _syncHandler;

    // may need JSON writer for errors:
    protected final ObjectWriter _jsonWriter;

    protected final AtomicBoolean _terminated = new AtomicBoolean(false);

    protected final OperationMetrics _listMetrics;
    
    public SyncListServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, null);
        _syncHandler = h;
        _jsonWriter = stuff.jsonWriter();
        final ServiceConfig serviceConfig = stuff.getServiceConfig();
        if (serviceConfig.metricsEnabled) {
            _listMetrics = OperationMetrics.forListingOperation(serviceConfig, "syncList");
        } else {
            _listMetrics = null;
        }
    }

    @Override
    public void destroy() {
        _terminated.set(true);
        super.destroy();
    }

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics) {
        metrics.SYNCLIST = ExternalOperationMetrics.create(_listMetrics);
    }

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        final OperationMetrics metrics = _listMetrics;
        Context timer = (metrics == null) ? null : metrics.start();
        String str = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_SINCE);
        try {
            if (str == null) {
                response = _syncHandler.missingArgument(response, ClusterMateConstants.QUERY_PARAM_SINCE);
            } else {
                long since = -1;
                try {
                    since = Long.parseLong(str);
                } catch (NumberFormatException e) { }
                if (since < 0L) {
                    response = _syncHandler.invalidArgument(response, ClusterMateConstants.QUERY_PARAM_SINCE, str);
                } else {
                    try {
                        response = _syncHandler.listEntries(request, response, since, stats);
                    } catch (IllegalStateException e) {
                        // Swallow during shutdown
                        if (!_terminated.get()) {
                            LOG.error("Failed syncHandler.listEntries(): "+e.getMessage(), e);
                        }
                        response.internalError("Failed syncHandler.listEntries(): "+e.getMessage());
                    } catch (InterruptedException e) {
                        // Swallow during shutdown (mostly during tests)
                        if (_terminated.get()) {
                            LOG.info("SyncListServlet interrupted due to termination, ignoring");
                        } else {
                            reportUnexpectedInterruptForListEntries();
                        }
                        return;
                    }
                    _addStdHeaders(response);
                }
            }
            response.writeOut(_jsonWriter);
        } finally {
            if (metrics != null) {
                metrics.finish(timer, stats);
           }
        }
    }

    protected void reportUnexpectedInterruptForListEntries()
        throws IOException
    {
        /* 08-Jan-2013, tatu: This seems to occur during shutdowns, should
         *   find a way to pipe shutdown notifications to servlet too.
         *   But for now, let's just customize message a bit:
         */
        throw new IOException("syncHandler.listEntries() interrupted, system not yet shut down: probably harmless");
    }
}
