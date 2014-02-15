package com.fasterxml.clustermate.servlet;

import java.io.IOException;

import com.codahale.metrics.Timer.Context;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalOperationMetrics;
import com.fasterxml.clustermate.service.metrics.OperationMetrics;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Servlet that handles access to per-entry metadata.
 */
@SuppressWarnings("serial")
public class StoreEntryInfoServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletWithMetricsBase
{
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */

    protected final SharedServiceStuff _stuff;
    
    protected final StoreHandler<K,E,?> _storeHandler;

    protected final ObjectWriter _jsonWriter;

    protected final EntryKeyConverter<K> _keyConverter;

    protected final StoredEntryConverter<K,E,?> _entryConverter;

    /*
    /**********************************************************************
    /* Metrics info
    /**********************************************************************
     */

    protected final OperationMetrics _getMetrics;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public StoreEntryInfoServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            StoreHandler<K,E,?> storeHandler)
    {
        this(stuff, clusterView, storeHandler, false);
    }

    protected StoreEntryInfoServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            StoreHandler<K,E,?> storeHandler, boolean handleRouting)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, null);
        _stuff = stuff;
        _storeHandler = storeHandler;
        _jsonWriter = stuff.jsonWriter();
        _entryConverter = stuff.getEntryConverter();
        _keyConverter = _entryConverter.keyConverter();
        ServiceConfig serviceConfig = stuff.getServiceConfig();
        if (serviceConfig.metricsEnabled) {
            _getMetrics = OperationMetrics.forEntityOperation(serviceConfig, "entryInfoGet");
        } else {
            _getMetrics = null;
        }
    }

    protected StoreEntryInfoServlet(StoreEntryInfoServlet<K,E> base,
            boolean copyMetrics)
    {
        super(base._stuff, base._clusterView, null);
        _stuff = base._stuff;
        _storeHandler = base._storeHandler;
        _jsonWriter = base._jsonWriter;
        _entryConverter = base._entryConverter;
        _keyConverter = base._keyConverter;
        if (copyMetrics) {
            _getMetrics = base._getMetrics;
        } else {
            _getMetrics = null;
        }
    }

    // TODO, maybe?
    /*
    public ServletBase createRoutingServlet() {
        return new RoutingEntryInfoServlet<K,E>(this);
    }
    */

    /*
    /**********************************************************************
    /* Access to metrics (AllOperationMetrics.Provider impl)
    /**********************************************************************
     */

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics)
    {
        metrics.GET = ExternalOperationMetrics.create(_getMetrics);
        _storeHandler.augmentOperationMetrics(metrics);
    }
    
    /*
    /**********************************************************************
    /* Default implementations for key handling
    /**********************************************************************
     */

    protected K _findKey(ServletServiceRequest request, ServletServiceResponse response)
    {
        return _keyConverter.extractFromPath(request);
    }
    
    /*
    /**********************************************************************
    /* Main Verb handlers
    /**********************************************************************
     */

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        final OperationMetrics metrics = _getMetrics;
        Context timer = (metrics == null) ? null : metrics.start();
        try {
            K key = _findKey(request, response);
            if (key != null) { // null means trouble; response has all we need
                response = _handleGet(request, response, stats, key);
            }
            response.writeOut(_jsonWriter);
        } finally {
            if (metrics != null) {
                 metrics.finish(timer, stats);
            }
        }
    }

    // Do we even need to support HEAD? Might as well, I guess
    @Override
    public void handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        K key = _findKey(request, response);
        if (key != null) {
            response = _handleHead(request, response, stats, key);
        }
        // note: should be enough to just add headers; no content to write
    }

    /*
    /**********************************************************************
    /* Handlers for actual operations, overridable
    /**********************************************************************
     */

    protected ServletServiceResponse _handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        Storable entry = _storeHandler.findRawEntryForInfo(key, stats);
        if (entry == null) {
            return response.notFound();
        }
        response = response.setContentTypeJson()
                .ok(_entryConverter.itemInfoFromStorable(entry));
        _addStdHeaders(response);
        return response;
    }

    protected ServletServiceResponse _handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        Storable entry = _storeHandler.findRawEntryForInfo(key, stats);
        if (entry == null) {
            return response.notFound();
        }
        // What kind of info, if any, should we return? Content-length would be possible,
        // but misleading/inaccurate, since our payload is JSON, not stored entry
        response = response.ok();
        _addStdHeaders(response);
        return response;
    }
}
