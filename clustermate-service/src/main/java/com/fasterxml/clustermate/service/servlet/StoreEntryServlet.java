package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.Storable;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

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

    protected final ServiceConfig _serviceConfig;
    
    protected final StoreHandler<K,E,?> _storeHandler;

    protected final TimeMaster _timeMaster;

    protected final ObjectWriter _jsonWriter;

    protected final EntryKeyConverter<K> _keyConverter;

    /*
    /**********************************************************************
    /* Metrics info
    /**********************************************************************
     */

    private final static Counter _metricGetReqCurrent = Metrics.newCounter(
            StoreEntryServlet.class, "getReqInFlight");

    private final static Histogram _metricGetReqSizes = Metrics.newHistogram(
            StoreEntryServlet.class, "getReqSizes", true);

    private final static Meter _metricGetReqRate = Metrics.newMeter(
            StoreEntryServlet.class, "getReqRate", "requests", TimeUnit.SECONDS);

    private final static Timer _metricGetReqTimes = Metrics.newTimer(
            StoreEntryServlet.class, "getReqTimes");
    
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
        _serviceConfig = stuff.getServiceConfig();
    }

    protected StoreEntryServlet(StoreEntryServlet<K,E> base)
    {
        super(base._clusterView, null);
        _storeHandler = base._storeHandler;
        _timeMaster = base._timeMaster;
        _jsonWriter = base._jsonWriter;
        _keyConverter = base._keyConverter;
        _serviceConfig = base._serviceConfig;
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
/*
 *     (non-Javadoc)
 * @see com.fasterxml.clustermate.service.servlet.ServletBase#handleGet(com.fasterxml.clustermate.service.servlet.ServletServiceRequest, com.fasterxml.clustermate.service.servlet.ServletServiceResponse, com.fasterxml.clustermate.service.OperationDiagnostics)
 */
    @Override
    public final void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        final boolean updateMetrics = _serviceConfig.metricsEnabled;
        final TimerContext timer;

        if (updateMetrics) {
            _metricGetReqCurrent.inc();
            _metricGetReqRate.mark();            
            timer = _metricGetReqTimes.time();
        } else {
            timer = null;
        }

        try {
            K key = _findKey(request, response);
            if (key != null) { // null means trouble; response has all we need
                _handleGet(request, response, stats, key);
            }
            response.writeOut(_jsonWriter);
        } finally {
            if (updateMetrics) {
                timer.stop();
                _metricGetReqCurrent.dec();
                if (stats != null) {
                    Storable entity = stats.getEntry();
                    if (entity != null) {
                        _metricGetReqSizes.update(entity.getActualUncompressedLength());
                        /*
                    } else {
                        _metricGetReqSizes.update(333);
                        */
                    }
                }
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
        K key = _findKey(request, response);
        if (key != null) {
            _handlePut(request, response, stats, key);
        }
        response.writeOut(_jsonWriter);
    }

    @Override
    public void handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats) throws IOException
    {
        K key = _findKey(request, response);
        if (key != null) {
            _handleDelete(request, response, stats, key);
        }
        response.writeOut(_jsonWriter);
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
