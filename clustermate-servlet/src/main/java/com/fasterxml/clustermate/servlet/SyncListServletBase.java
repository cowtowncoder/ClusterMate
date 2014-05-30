package com.fasterxml.clustermate.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.Timer.Context;
import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.OperationMetrics;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

abstract class SyncListServletBase<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletWithMetricsBase
{
    private static final long serialVersionUID = 1L;

    protected final SyncHandler<K,E> _syncHandler;

    // may need JSON writer for errors:
    protected final ObjectWriter _jsonWriter;

    protected final AtomicBoolean _terminated = new AtomicBoolean(false);

    protected final OperationMetrics _listMetrics;

    protected final String _loggedMethodName;

    public SyncListServletBase(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h, String metricsName, String loggedMethodName)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, null);
        _syncHandler = h;
        _jsonWriter = stuff.jsonWriter();
        _loggedMethodName = "_syncHandler."+loggedMethodName;
        final ServiceConfig serviceConfig = stuff.getServiceConfig();
        if (serviceConfig.metricsEnabled) {
            _listMetrics = OperationMetrics.forListingOperation(serviceConfig, metricsName);
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
    public abstract void fillOperationMetrics(AllOperationMetrics metrics);

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
                        response = listEntries(request, response, since, stats);
                    } catch (IllegalStateException e) {
                        // Swallow during shutdown
                        if (!_terminated.get()) {
                            LOG.error("Failed call to "+_loggedMethodName+": "+e.getMessage(), e);
                        }
                        response.internalError("Failed call to "+_loggedMethodName+": "+e.getMessage());
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

    protected abstract ServletServiceResponse listEntries(ServletServiceRequest request,
            ServletServiceResponse response,
            long since, OperationDiagnostics stats) throws IOException, InterruptedException;
    
    protected void reportUnexpectedInterruptForListEntries() throws IOException
    {
        /* 08-Jan-2013, tatu: This seems to occur during shutdowns, should
         *   find a way to pipe shutdown notifications to servlet too.
         *   But for now, let's just customize message a bit:
         */
        throw new IOException(_loggedMethodName+" interrupted, system not yet shut down: probably harmless");
    }
}
