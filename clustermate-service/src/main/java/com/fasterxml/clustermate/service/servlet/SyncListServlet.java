package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.EntryKey;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;

@SuppressWarnings("serial")
public class SyncListServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletBase
{
    protected final SyncHandler<K,E> _syncHandler;

    // may need JSON writer for errors:
    protected final ObjectWriter _jsonWriter;

    public SyncListServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            SyncHandler<K,E> h)
    {
        // null -> use servlet path base as-is
        super(clusterView, null);
        _syncHandler = h;
        _jsonWriter = stuff.jsonWriter();
    }

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        String str = request.getQueryParameter(ClusterMateConstants.HTTP_QUERY_PARAM_SINCE);
        if (str == null) {
            response = _syncHandler.missingArgument(response, ClusterMateConstants.HTTP_QUERY_PARAM_SINCE);
        } else {
            long since = -1;
            try {
                since = Long.parseLong(str);
            } catch (NumberFormatException e) { }
            if (since < 0L) {
                response = _syncHandler.invalidArgument(response, ClusterMateConstants.HTTP_QUERY_PARAM_SINCE, str);
            } else {
                response = _syncHandler.listEntries(request, response, since, stats);
                _addStdHeaders(response);
            }
        }
        response.writeOut(_jsonWriter);
    }
}
