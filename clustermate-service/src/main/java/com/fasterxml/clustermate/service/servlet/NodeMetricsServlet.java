package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Stand-alone servlet that may be used for serving metrics information
 * regarding this node (and possibly its peers as well)
 */
@SuppressWarnings("serial")
public class NodeMetricsServlet extends ServletBase
{
    protected final ClusterInfoHandler _handler;

    protected final ObjectWriter _jsonWriter;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public NodeMetricsServlet(SharedServiceStuff stuff, ClusterInfoHandler h)
    {
        // null -> use servlet path base as-is
        super(null, null);
        _handler = h;
        _jsonWriter = stuff.jsonWriter();
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
        // TODO:
        super.handleGet(request, response, metadata);
        
        /*
        response = _handler.getStatus(request, response, metadata);
        response.writeOut(null);
        */
    }
}
