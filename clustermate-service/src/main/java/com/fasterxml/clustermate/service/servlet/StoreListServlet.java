package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.storemate.shared.TimeMaster;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

@SuppressWarnings("serial")
public class StoreListServlet<K extends EntryKey,
    E extends StoredEntry<K>
>
    extends ServletBase
{
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */
        
//        private final Log LOG = Log.forClass(getClass());

    protected final StoreHandler<K,E,?> _storeHandler;

    protected final TimeMaster _timeMaster;

    protected final EntryKeyConverter<K> _keyConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public StoreListServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            StoreHandler<K,E,?> storeHandler)
    {
        // null -> use servlet path base as-is
        super(clusterView, null);
        _storeHandler = storeHandler;
        _timeMaster = stuff.getTimeMaster();
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
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        K prefix = _findKey(request, response);
        if (prefix == null) {
            super.handleGet(request, response, stats);
            return;
        }
        _storeHandler.listEntries(request, response, prefix, stats);
        _addStdHeaders(response);
        response.writeOut(null);
    }
}
