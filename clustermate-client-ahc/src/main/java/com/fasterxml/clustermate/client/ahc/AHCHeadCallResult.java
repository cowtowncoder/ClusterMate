package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.HeadCallResult;
import com.ning.http.client.HttpResponseHeaders;

public class AHCHeadCallResult extends HeadCallResult
{
    protected HttpResponseHeaders _headers;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public AHCHeadCallResult(ClusterServerNode server, long contentLength) {
        super(server, contentLength);
    }

    public AHCHeadCallResult(ClusterServerNode server, int statusCode, long contentLength) {
        super(server, statusCode, contentLength);
    }
    
    public AHCHeadCallResult(CallFailure fail) {
        super(fail);
    }

    public static AHCHeadCallResult notFound(ClusterServerNode server) {
        return new AHCHeadCallResult(server, ClusterMateConstants.HTTP_STATUS_NOT_FOUND, -1);
    }

    public void setHeaders(HttpResponseHeaders h) {
        _headers = h;
    }
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

    @Override
    public String getHeaderValue(String key)
    {
        HttpResponseHeaders h = _headers;
        return (h == null) ? null : h.getHeaders().getFirstValue(key);
    }
}
