package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

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
import com.yammer.metrics.core.TimerContext;

@SuppressWarnings("serial")
public class StoreListServlet<K extends EntryKey,
    E extends StoredEntry<K>
>
    extends MetricsServletBase
{
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */

    protected final StoreHandler<K,E,?> _storeHandler;

    protected final TimeMaster _timeMaster;

    protected final EntryKeyConverter<K> _keyConverter;

    /*
    /**********************************************************************
    /* Metrics info
    /**********************************************************************
     */

    protected final OperationMetrics _listMetrics;
    
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

        final ServiceConfig serviceConfig = stuff.getServiceConfig();
        if (serviceConfig.metricsEnabled) {
            _listMetrics = OperationMetrics.forListingOperation(serviceConfig, "entryList");
        } else {
            _listMetrics = null;
        }
    }

    @Override
    public void fillOperationMetrics(AllOperationMetrics metrics) {
        metrics.LIST = ExternalOperationMetrics.create(_listMetrics);
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
        final OperationMetrics metrics = _listMetrics;
        TimerContext timer = (metrics == null) ? null : metrics.start();
        try {
            K prefix = _findKey(request, response);
            if (prefix == null) {
                super.handleGet(request, response, stats);
                return;
            }
            _storeHandler.listEntries(request, response, prefix, stats);
            _addStdHeaders(response);
            response.writeOut(null);
        } finally {
            if (metrics != null) {
                 metrics.finish(timer, stats);
            }
        }
    }
}
