package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Stand-alone servlet that may be used for serving node state information;
 * usually not used, but can be used.
 */
@SuppressWarnings("serial")
public class NodeStatusServlet extends ServletBase
{
    protected final ClusterInfoHandler _handler;

    protected final ObjectWriter _jsonWriter;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public NodeStatusServlet(SharedServiceStuff stuff, ClusterInfoHandler h)
    {
        // null -> use servlet path base as-is
        super(null, null);
        _handler = h;
        _jsonWriter = stuff.jsonWriter();
    }

    /*
    /**********************************************************************
    /* API: returning Node status
    /**********************************************************************
     */

    /**
     * GET is used for simple access of cluster status
     */
    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        response = _handler.getStatus(request, response, metadata);
        response.writeOut(null);
    }

    /**
     * POST is used for simple hello/goodbye style notifications.
     */
    @Override
    public void handlePost(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        response = _handler.handlePost(request, response, metadata);
        response.writeOut(_jsonWriter);
    }
}
