package com.fasterxml.clustermate.servlet;

import java.io.IOException;

import com.fasterxml.storemate.store.util.OperationDiagnostics;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalOperationMetrics;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;

/**
 * Servlet that handles "sync-pull" requests by peer nodes of the same (local)
 * cluster.
 */
public class SyncPullServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends SyncPullServletBase<K,E>
{
    private static final long serialVersionUID = 1L;

    public SyncPullServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, h, "SyncPull");
    }

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics) {
        metrics.SYNCPULL = ExternalOperationMetrics.create(_pullMetrics);
    }

    @Override
    protected ServletServiceResponse _pullEntries(ServletServiceRequest request,
            ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        return _syncHandler.localPullEntries(request, response, request.getInputStream(), metadata);
    }
}
