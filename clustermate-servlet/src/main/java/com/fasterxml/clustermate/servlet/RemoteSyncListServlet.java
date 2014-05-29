package com.fasterxml.clustermate.servlet;

import java.io.IOException;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalOperationMetrics;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

/**
 * Handler for "remote" calls to sync-list, used for cluster-to-cluster
 * synchronization of content.
 */
public class RemoteSyncListServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends SyncListServletBase<K,E>
{
    private static final long serialVersionUID = 1L;

    public RemoteSyncListServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        super(stuff, clusterView, h, "remoteSyncList", "remoteListEntries()");
    }

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics) {
        metrics.REMOTE_SL = ExternalOperationMetrics.create(_listMetrics);
    }

    @Override
    protected ServletServiceResponse listEntries(ServletServiceRequest request,
            ServletServiceResponse response,
            long since, OperationDiagnostics stats) throws IOException, InterruptedException {
        return _syncHandler.remoteListEntries(request, response, since, stats);
    }
}
