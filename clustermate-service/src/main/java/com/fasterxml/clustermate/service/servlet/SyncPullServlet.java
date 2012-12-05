package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.EntryKey;

import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;

@SuppressWarnings("serial")
public class SyncPullServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletBase
{
    protected final SyncHandler<K,E> _syncHandler;

    // may need JSON writer for errors:
    protected final ObjectWriter _jsonWriter;
    
    public SyncPullServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        // null -> use servlet path base as-is
        super(clusterView, null);
        _syncHandler = h;
        _jsonWriter = stuff.jsonWriter();
    }
    
    @Override
    public void handlePost(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        _syncHandler.pullEntries(request, response, request.getInputStream(), metadata);
        _addStdHeaders(response);
        // pass metadata to track number of bytes returned...
        response.writeOut(_jsonWriter, metadata);
    }

}
