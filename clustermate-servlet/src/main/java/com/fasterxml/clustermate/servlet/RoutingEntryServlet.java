package com.fasterxml.clustermate.servlet;

import java.io.IOException;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

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
    /* Overridden handler methods
    /**********************************************************************
     */

    @Override
    protected ServletServiceResponse _handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        NodeState node = _findRedirect(request, response, key);
        if (node == null) {
            response = (ServletServiceResponse) _storeHandler.getEntry(request, response, key, stats);
            _addStdHeaders(response);
            return response;
        }
        // TODO: routing!
        throw new IllegalStateException("Not yet implemented!");
    }

    @Override
    protected ServletServiceResponse _handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        NodeState node = _findRedirect(request, response, key);
        if (node == null) {
            _storeHandler.getEntryStats(request, response, key, stats);
            _addStdHeaders(response);
            return response;
        }
        // TODO: routing!
        throw new IllegalStateException("Not yet implemented!");
    }

    @Override
    protected ServletServiceResponse _handlePut(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        NodeState node = _findRedirect(request, response, key);
        if (node == null) {
            _storeHandler.putEntry(request, response, key, request.getInputStream(),
                    stats);
            _addStdHeaders(response);
            return response;
        }
        throw new IllegalStateException("Not yet implemented!");
        // TODO: routing!
    }

    @Override
    protected ServletServiceResponse _handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        NodeState node = _findRedirect(request, response, key);
        if (node == null) {
            _storeHandler.removeEntry(request, response, key, stats);
            _addStdHeaders(response);
            return response;
        }
        throw new IllegalStateException("Not yet implemented!");
        // TODO: routing!
    }

    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    protected NodeState _findRedirect(ServletServiceRequest request, ServletServiceResponse response,
            K key)
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

    protected boolean _isStoredLocally(K key) {
        return _clusterView.containsLocally(key);
    }
}
