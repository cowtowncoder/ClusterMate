package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.ClusterServerNode;

public abstract class ReadCallResult<T> extends CallResult
{
    protected final T _result;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */

    protected ReadCallResult(ClusterServerNode server, T result) {
        this(server, ClusterMateConstants.HTTP_STATUS_OK, result);
    }

    protected ReadCallResult(ClusterServerNode server, int status) {
        super(server, status);
        _result = null;
    }
    
    protected ReadCallResult(ClusterServerNode server, int status, T result) {
        super(server, status);
        _result = result;
    }

    protected ReadCallResult(CallFailure fail) {
        super(fail);
        _result = null;
    }

    /*
    /**********************************************************************
    /* CallResult impl
    /**********************************************************************
     */

    @Override
    public abstract String getHeaderValue(String key);
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */
    
    public T getResult() { return _result; }
}
