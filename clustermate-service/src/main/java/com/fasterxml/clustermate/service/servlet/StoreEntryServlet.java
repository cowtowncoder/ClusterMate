package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.TimeMaster;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Servlet that handles basic CRUD operations for individual entries.
 */
@SuppressWarnings("serial")
public class StoreEntryServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletBase
{
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */
    
//    private final Log LOG = Log.forClass(getClass());

    protected final StoreHandler<K,E> _storeHandler;

    protected final TimeMaster _timeMaster;

    protected final ObjectWriter _jsonWriter;

    protected final EntryKeyConverter<K> _keyConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public StoreEntryServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            StoreHandler<K,E> storeHandler)
    {
        // null -> use servlet path base as-is
        super(clusterView, null);
        _storeHandler = storeHandler;
        _timeMaster = stuff.getTimeMaster();
        _jsonWriter = stuff.jsonWriter();
        _keyConverter = stuff.getKeyConverter();
    }

    /*
    /**********************************************************************
    /* Default implementation for key handling
    /**********************************************************************
     */

    protected K _findKey(ServletServiceRequest request, ServletServiceResponse response)
    {
        return _keyConverter.extractFromPath(request);
    }
    
    /*
    /**********************************************************************
    /* Delegated methods for more control
    /**********************************************************************
     */

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response) throws IOException
    {
        K key = _findKey(request, response);
        if (key != null) { // null means trouble; response has all we need
            _storeHandler.getEntry(request, response, key);
            _addStdHeaders(response);
        }
        response.writeOut(_jsonWriter);
    }

    @Override
    public void handleHead(ServletServiceRequest request, ServletServiceResponse response) throws IOException
    {
        K key = _findKey(request, response);
        if (key != null) {
            _storeHandler.getEntryStats(request, response, key);
            _addStdHeaders(response);
        }
        // note: should be enough to just add headers; no content to write
    }

    // We'll allow POST as an alias to PUT
    @Override
    public void handlePost(ServletServiceRequest request, ServletServiceResponse response) throws IOException
    {
        handlePut(request, response);
    }
    
    @Override
    public void handlePut(ServletServiceRequest request, ServletServiceResponse response) throws IOException
    {
        K key = _findKey(request, response);
        if (key != null) {
            _storeHandler.putEntry(request, response, key,
                    request.getNativeRequest().getInputStream());
            _addStdHeaders(response);
        }
        response.writeOut(_jsonWriter);
    }

    @Override
    public void handleDelete(ServletServiceRequest request, ServletServiceResponse response) throws IOException
    {
        K key = _findKey(request, response);
        if (key != null) {
            _storeHandler.removeEntry(request, response, key);
            _addStdHeaders(response);
        }
        response.writeOut(_jsonWriter);
    }
}
