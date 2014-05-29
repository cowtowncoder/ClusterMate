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
 * Servlet that handles "sync-pull" requests by nodes of remote clusters.
 */
public class RemoteSyncPullServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends SyncPullServletBase<K,E>
{
    private static final long serialVersionUID = 1L;

    public RemoteSyncPullServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, h, "remoteSyncPull");
    }

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics) {
        metrics.REMOTE_SP = ExternalOperationMetrics.create(_pullMetrics);
    }

    @Override
    protected ServletServiceResponse _pullEntries(ServletServiceRequest request,
            ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        return _syncHandler.remotePullEntries(request, response, request.getInputStream(), metadata);
    }
}
