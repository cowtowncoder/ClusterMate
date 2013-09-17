package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.metrics.BackgroundMetricsAccessor;
import com.fasterxml.clustermate.service.metrics.SerializedMetrics;

/**
 * Stand-alone servlet that may be used for serving metrics information
 * regarding this node (and possibly its peers as well)
 */
@SuppressWarnings("serial")
public class NodeMetricsServlet extends ServletBase
{
    /**
     * Query parameter to request server to refresh its stats
     */
    protected final static String QUERY_PARAM_REFRESH = "refresh";

    /**
     * Query parameter to further request server to obtain full
     * statistics, not just cheap ones.
     */
    protected final static String QUERY_PARAM_FULL = "full";

    /**
     * Can also explicitly request indentation (or lack thereof)
     */
    protected final static String QUERY_PARAM_INDENT = "indent";
    
    protected final BackgroundMetricsAccessor _accessor;

    // Let's NOT indent by default. To save bandwidth
    protected final AtomicBoolean _indent = new AtomicBoolean(false);
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    /*
    SharedServiceStuff stuff, Stores<?,?> stores,
            AllOperationMetrics.Provider[] metricsProviders)
     */
    
    public NodeMetricsServlet(SharedServiceStuff stuff, BackgroundMetricsAccessor accessor)
    {
        // null -> use servlet path base as-is
        super(stuff, null, null);
        _accessor = accessor;
    }

    public void setIndent(boolean state) {
        _indent.set(state);
    }
    
    /*
    /**********************************************************************
    /* End points: for now just GET
    /**********************************************************************
     */
    
    /**
     * GET is used for simple access of cluster status
     */
    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        try {
            // One more thing: is caller trying to force refresh?
            boolean forceRefresh = "true".equals(request.getQueryParameter(QUERY_PARAM_REFRESH));
            boolean full = "true".equals(request.getQueryParameter(QUERY_PARAM_FULL));
            boolean indent = _indent.get();
            // or, enable/disable indentation explicitly
            String indentStr = request.getQueryParameter(QUERY_PARAM_INDENT);
            if (indentStr != null && indentStr.length() > 0) {
                indent = Boolean.valueOf(indentStr.trim());
            }
            SerializedMetrics ser = _accessor.getMetrics(forceRefresh, full);
            response = (ServletServiceResponse) response.ok()
                    .setContentTypeJson();
            response.writeRaw(indent ? ser.getIndented() : ser.getRaw());
        } catch (Exception e) {
            String msg = "Failed to serialize Metrics: "+e;
            LOG.warn(msg, e);
            response = (ServletServiceResponse) response
                .internalError(msg)
                .setContentTypeText()
                ;
            response.writeOut(null);
        }
    }
}
