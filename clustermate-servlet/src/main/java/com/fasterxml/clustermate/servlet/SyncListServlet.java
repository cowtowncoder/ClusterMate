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

public class SyncListServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends SyncListServletBase<K,E>
{
    private static final long serialVersionUID = 1L;

    public SyncListServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        super(stuff, clusterView, h, "localSyncList", "localListEntries()");
    }

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics) {
        metrics.SYNCLIST = ExternalOperationMetrics.create(_listMetrics);
    }

    @Override
    protected ServletServiceResponse listEntries(ServletServiceRequest request,
            ServletServiceResponse response,
            long since, OperationDiagnostics stats) throws IOException, InterruptedException {
        return _syncHandler.localListEntries(request, response, since, stats);
    }
}
