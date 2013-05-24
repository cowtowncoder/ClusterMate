package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.yammer.metrics.core.TimerContext;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.TimeMaster;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalOperationMetrics;
import com.fasterxml.clustermate.service.metrics.OperationMetrics;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Servlet that handles basic CRUD operations for individual entries.
 */
@SuppressWarnings("serial")
public class StoreEntryServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletWithMetricsBase
{
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */

//    protected final ServiceConfig _serviceConfig;
    
    protected final StoreHandler<K,E,?> _storeHandler;

    protected final TimeMaster _timeMaster;

    protected final ObjectWriter _jsonWriter;

    protected final EntryKeyConverter<K> _keyConverter;

    /*
    /**********************************************************************
    /* Metrics info
    /**********************************************************************
     */

    protected final OperationMetrics _getMetrics;

    protected final OperationMetrics _putMetrics;

    protected final OperationMetrics _deleteMetrics;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public StoreEntryServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            StoreHandler<K,E,?> storeHandler)
    {
        this(stuff, clusterView, storeHandler, false);
    }

    protected StoreEntryServlet(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            StoreHandler<K,E,?> storeHandler, boolean handleRouting)
    {
        // null -> use servlet path base as-is
        super(clusterView, null);
        _storeHandler = storeHandler;
        _timeMaster = stuff.getTimeMaster();
        _jsonWriter = stuff.jsonWriter();
        _keyConverter = stuff.getKeyConverter();
        ServiceConfig serviceConfig = stuff.getServiceConfig();
        if (serviceConfig.metricsEnabled) {
            _getMetrics = OperationMetrics.forEntityOperation(serviceConfig, "entryGet");
            _putMetrics = OperationMetrics.forEntityOperation(serviceConfig, "entryPut");
            _deleteMetrics = OperationMetrics.forNonPayloadOperation(serviceConfig, "entryDelete");
        } else {
            _getMetrics = null;
            _putMetrics = null;
            _deleteMetrics = null;
        }
    }

    protected StoreEntryServlet(StoreEntryServlet<K,E> base,
            boolean copyMetrics)
    {
        super(base._clusterView, null);
        _storeHandler = base._storeHandler;
        _timeMaster = base._timeMaster;
        _jsonWriter = base._jsonWriter;
        _keyConverter = base._keyConverter;
        if (copyMetrics) {
            _getMetrics = base._getMetrics;
            _putMetrics = base._putMetrics;
            _deleteMetrics = base._deleteMetrics;
        } else {
            _getMetrics = null;
            _putMetrics = null;
            _deleteMetrics = null;
        }
    }
    
    /**
     * "Mutant factory" method used to create "routing" version of this servlet:
     * this will basically handle request locally (as t
     */
    public ServletBase createRoutingServlet() {
        return new RoutingEntryServlet<K,E>(this);
    }

    /*
    /**********************************************************************
    /* Access to metrics (AllOperationMetrics.Provider impl)
    /**********************************************************************
     */

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics)
    {
        metrics.GET = ExternalOperationMetrics.create(_getMetrics);
        metrics.PUT = ExternalOperationMetrics.create(_putMetrics);
        metrics.DELETE = ExternalOperationMetrics.create(_deleteMetrics);
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
    public final void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        final OperationMetrics metrics = _getMetrics;
        TimerContext timer = (metrics == null) ? null : metrics.start();
        try {
            K key = _findKey(request, response);
            if (key != null) { // null means trouble; response has all we need
                _handleGet(request, response, stats, key);
            }
            response.writeOut(_jsonWriter);
        } finally {
            if (metrics != null) {
                 metrics.finish(timer, stats);
            }
        }
    }

    @Override
    public final void handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        K key = _findKey(request, response);
        if (key != null) {
            _handleHead(request, response, stats, key);
        }
        // note: should be enough to just add headers; no content to write
    }

    // We'll allow POST as an alias to PUT
    @Override
    public final void handlePost(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        handlePut(request, response, stats);
    }
    
    @Override
    public final void handlePut(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        final OperationMetrics metrics = _putMetrics;
        TimerContext timer = (metrics == null) ? null : metrics.start();

        try {
            K key = _findKey(request, response);
            if (key != null) {
                _handlePut(request, response, stats, key);
            }
            response.writeOut(_jsonWriter);
        } finally {
            if (metrics != null) {
                 metrics.finish(timer, stats);
            }
        }
    }

    @Override
    public void handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        final OperationMetrics metrics = _deleteMetrics;
        TimerContext timer = (metrics == null) ? null : metrics.start();

        try {
            K key = _findKey(request, response);
            if (key != null) {
                _handleDelete(request, response, stats, key);
            }
            response.writeOut(_jsonWriter);
        } finally {
            if (metrics != null) {
                metrics.finish(timer, stats);
            }
        }
    }

    /*
    /**********************************************************************
    /* Handlers for actual operations, overridable
    /**********************************************************************
     */

    protected void _handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.getEntry(request, response, key, stats);
        _addStdHeaders(response);
    }

    protected void _handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.getEntryStats(request, response, key, stats);
        _addStdHeaders(response);
    }

    protected void _handlePut(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.putEntry(request, response, key,
                request.getNativeRequest().getInputStream(),
                stats);
        _addStdHeaders(response);
    }

    protected void _handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats, K key)
        throws IOException
    {
        _storeHandler.removeEntry(request, response, key, stats);
        _addStdHeaders(response);
    }    
}
