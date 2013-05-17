package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Specialized sub-type of Entry servlet that will either serve request
 * locally (if data available), or return redirect code to indicate
 * client who to call instead.
 *<p>
 * TODO: complete -- not yet ready
 */
@SuppressWarnings("serial")
public class RoutingEntryServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends StoreEntryServlet<K,E>
{
    public RoutingEntryServlet(StoreEntryServlet<K,E> base)
    {
        // true -> share metrics with the "real" servlet. Can separate in future if need be
        super(base, true);
    }
    
    /*
    /**********************************************************************
    /* Overridable helper methods
    /**********************************************************************
     */
    
    protected boolean _isStoredLocally(K key) {
        return _clusterView.containsLocally(key);
    }

    protected int _findRetryCount(ServletServiceRequest request)
    {
        String str = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_RETRY_COUNT);
        if (str != null) {
            str = str.trim();
            if (str.length() > 0) {
                try {
                    return Integer.parseInt(str);
                } catch (NumberFormatException e) { }
            }
        }
        return -1;
    }

    /*
    /**********************************************************************
    /* Overriden handler methods
    /**********************************************************************
     */

    @Override
    protected void _handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.getEntry(request, response, key, stats);
        _addStdHeaders(response);
    }

    @Override
    protected void _handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.getEntryStats(request, response, key, stats);
        _addStdHeaders(response);
    }

    @Override
    protected void _handlePut(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.putEntry(request, response, key,
                request.getNativeRequest().getInputStream(),
                stats);
        _addStdHeaders(response);
    }

    @Override
    protected void _handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.removeEntry(request, response, key, stats);
        _addStdHeaders(response);
    }

    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    protected NodeState _findRedirect(ServletServiceRequest request, ServletServiceResponse response, K key)
    {
        // First: retry count always affects things
        int retry = _findRetryCount(request);
        // except for one case; first try, and we have entry locally
        boolean hasLocally = _isStoredLocally(key);
        if (hasLocally && (retry == 0)) {
            return null;
        }
        // otherwise need to find ordered set of targets
        
        // !!! TODO
        return null;
    }
}
