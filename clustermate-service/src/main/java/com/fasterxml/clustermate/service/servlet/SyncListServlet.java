package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.yammer.metrics.core.TimerContext;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.OperationDiagnostics;
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
    extends MetricsServletBase
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
        super(clusterView, null);
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
        TimerContext timer = (metrics == null) ? null : metrics.start();
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
                        if (!_terminated.get()) {
                            throw new IOException(e);
                        }
                        LOG.info("SyncListServlet interupted due to termination, ignoring");
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
}
