package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.client.ClusterServerNode;

public abstract class HeadCallResult
    extends CallResult
{
    protected final long _contentLength;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    protected HeadCallResult(ClusterServerNode server, long contentLength) {
        super(server);
        _contentLength = contentLength;
    }

    protected HeadCallResult(ClusterServerNode server, int status, long contentLength) {
        super(server, status);
        _contentLength = contentLength;
    }

    protected HeadCallResult(CallFailure fail) {
        super(fail);
        _contentLength = -1;
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
    
    public long getContentLength() { return _contentLength; }
    public boolean hasContentLength() { return _contentLength >= 0L; }
}
