package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.ClusterMateConstants;

public abstract class ReadCallResult<T> extends CallResult
{
    protected final T _result;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */

    protected ReadCallResult(T result) {
        this(ClusterMateConstants.HTTP_STATUS_OK, result);
    }

    protected ReadCallResult(int status) {
        super(status);
        _result = null;
    }
    
    protected ReadCallResult(int status, T result) {
        super(status);
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
