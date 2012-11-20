package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.clustermate.api.ClusterStatusMessage;
import com.fasterxml.clustermate.service.ServiceRequest;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.http.StreamingEntityImpl;

/**
 * Handler that provides information about current cluster state,
 * as seen by this node
 */
public class ClusterInfoHandler
{
    protected final ClusterViewByServer _cluster;

    protected final ObjectWriter _writer;
    
    public ClusterInfoHandler(SharedServiceStuff stuff, ClusterViewByServer cluster)
    {
        _cluster = cluster;
        // Should we indent? Not for prod?
//        _writer = stuff.jsonWriter(ClusterStatusResponse.class).withDefaultPrettyPrinter();
        _writer = stuff.jsonWriter(ClusterStatusMessage.class);
    }
    
    /**
     * Simple read request for getting snapshot of Cluster status as seen
     * by this node.
     */
    @SuppressWarnings("unchecked")
    public <RESP extends ServiceResponse> RESP getStatus(ServiceRequest request, RESP response)
    {
        // use streaming impl just so we'll use specific ObjectWriter
        return (RESP) response.ok(new StreamingEntityImpl(_writer, _cluster.asMessage()))
            .setContentTypeJson();
    }
}
