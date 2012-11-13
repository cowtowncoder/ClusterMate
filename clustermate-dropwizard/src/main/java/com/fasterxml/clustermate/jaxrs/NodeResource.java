package com.fasterxml.clustermate.jaxrs;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.yammer.metrics.annotation.Timed;

import com.fasterxml.clustermate.api.OperationType;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;

/**
 * Interface used for handling Cluster state information; both
 * updates between nodes and status requests by clients.
 */
@Path("/v/node")
@Produces(MediaType.APPLICATION_JSON)
public class NodeResource
{
    protected final ClusterInfoHandler _handler;
    
    public NodeResource(ClusterInfoHandler h) {
        _handler = h;
    }

    /*
    /**********************************************************************
    /* Entry points
    /**********************************************************************
     */
    
    /**
     * Simple read request for getting snapshot of Cluster status as seen
     * by this node.
     */
    @GET
    @Path("status")
    @Timed
    public Response getStatus(@Context UriInfo uriInfo)
    {
        JaxrsHttpResponse response = new JaxrsHttpResponse();
        // do we need headers? Shouldnt?
        _handler.getStatus(new JaxrsHttpRequest(uriInfo, null, "", OperationType.GET), response);
        return response.buildResponse();
    }
}
