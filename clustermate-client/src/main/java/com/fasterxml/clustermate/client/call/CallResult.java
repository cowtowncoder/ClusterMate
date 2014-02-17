package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.ClusterServerNode;

public abstract class CallResult
{
    protected final ClusterServerNode _server;

    protected final int _status;

    protected final CallFailure _fail;

    protected CallResult(ClusterServerNode server) {
        this(server, ClusterMateConstants.HTTP_STATUS_OK, null);
    }
    
    protected CallResult(ClusterServerNode server, int statusCode) {
        this(server, statusCode, null);
    }

    protected CallResult(CallFailure fail) {
        this((ClusterServerNode) fail.getServer(), fail.getStatusCode(), fail);
    }
    
    protected CallResult(ClusterServerNode server, int statusCode, CallFailure fail)
    {
        _server = server;
        _status = statusCode;
        _fail = fail;
    }

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */
    
    public int getStatus() { return _status; }

    public abstract String getHeaderValue(String key);
    
    public boolean failed() { return _fail != null; }
    public boolean succeeded() { return !failed(); }

    public CallFailure getFailure() { return _fail; }

    public ClusterServerNode getServer() { return _server; }
}
