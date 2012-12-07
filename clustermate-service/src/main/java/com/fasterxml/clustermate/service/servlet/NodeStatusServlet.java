package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;

/**
 * Stand-alone servlet that may be used for serving node state information;
 * usually not used, but can be used.
 */
@SuppressWarnings("serial")
public class NodeStatusServlet extends ServletBase
{
    protected final ClusterInfoHandler _handler;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public NodeStatusServlet(ClusterInfoHandler h)
    {
        // null -> use servlet path base as-is
        super(null, null);
        _handler = h;
    }

    /*
    /**********************************************************************
    /* API: returning Node status
    /**********************************************************************
     */

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        response = _handler.getStatus(request, response, metadata);
        // since we are counting response bytes, do pass metadata here:
        response.writeOut(null);
    }

    /*
    /**********************************************************************
    /* TODO: API for sending pro-active updates? (Node shutting down)
    /**********************************************************************
     */
}
