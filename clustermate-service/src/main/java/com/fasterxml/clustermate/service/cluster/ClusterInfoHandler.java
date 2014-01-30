package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.msg.ErrorResponse;
import com.fasterxml.clustermate.service.msg.StreamingEntityImpl;

/**
 * Handler that provides information about current cluster state,
 * as seen by this node, as well as handles simple update notifications.
 */
public class ClusterInfoHandler
    extends HandlerBase
{
    protected final ClusterViewByServerUpdatable _cluster;

    protected final ObjectWriter _writer;
    
    public ClusterInfoHandler(SharedServiceStuff stuff, ClusterViewByServerUpdatable cluster)
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
    public <RESP extends ServiceResponse> RESP getStatus(ServiceRequest request, RESP response,
            OperationDiagnostics metadata)
    {
        // use streaming impl just so we'll use specific ObjectWriter
        return (RESP) response.ok(new StreamingEntityImpl(_writer, _cluster.asMessage()))
            .setContentTypeJson();
    }

    /**
     * Handler for notification POSTs
     */
    @SuppressWarnings("unchecked")
    public <RESP extends ServiceResponse> RESP handlePost(ServiceRequest request, RESP response,
            OperationDiagnostics metadata)
    {
        // First things first: need to have a caller
        IpAndPort caller = getCallerQueryParam(request);
        if (caller == null) {
            return badRequest(response,
                    "Missing or invalid '"+ClusterMateConstants.QUERY_PARAM_CALLER+"' value");
        }
        // as well as known state...
        String stateStr = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_STATE);
        // including timestamp by sender
        String timestampStr = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_TIMESTAMP);
        long timestamp = 0L;
        
        if (timestampStr != null) {
            try {
                timestamp = Long.parseLong(timestampStr);
            } catch (IllegalArgumentException e) { }
        }
        if (timestamp <= 0L) {
            return (RESP) badRequest(response, "Invalid '"+ClusterMateConstants.QUERY_PARAM_TIMESTAMP
                    +"': '{}'", timestampStr);
        }
        
        if (ClusterMateConstants.STATE_ACTIVE.equals(stateStr)) {
            KeyRange range = null;

            // For activation, would prefer having key range too:
            Integer keyRangeStart = _findIntParam(request, ClusterMateConstants.QUERY_PARAM_KEYRANGE_START);
            if (keyRangeStart != null) {
                Integer keyRangeLength = _findIntParam(request, ClusterMateConstants.QUERY_PARAM_KEYRANGE_LENGTH);
                if (keyRangeLength != null) {
                    try {
                        range = _cluster.getKeySpace().range(keyRangeStart, keyRangeLength);
                    } catch (Exception e) {
                        return (RESP) badRequest(response, "Invalid key-range definition (start '%s', end '%s'): %s",
                                keyRangeStart, keyRangeLength, e.getMessage());
                    }
                }
            }
            if (range == null) {
                range = _cluster.getKeySpace().emptyRange();
            }
            _cluster.nodeActivated(caller, timestamp, range);
        } else if (ClusterMateConstants.STATE_INACTIVE.equals(stateStr)) {
            _cluster.nodeDeactivated(caller, timestamp);
        } else {
            return badRequest(response,
                    "Unrecognized '"+ClusterMateConstants.QUERY_PARAM_STATE+"' value '"+stateStr+"'");
        }
        return (RESP) response.ok();
    }

    /*
    /**********************************************************************
    /* Helper methods, error responses
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    protected <OUT extends ServiceResponse> OUT _badRequest(ServiceResponse response, String msg) {
        return (OUT) response
                .badRequest(new ErrorResponse(msg))
                .setContentTypeJson();
    }
}
