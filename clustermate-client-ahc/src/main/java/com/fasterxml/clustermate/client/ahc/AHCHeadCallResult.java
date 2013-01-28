package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.client.CallFailure;
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
    
    public AHCHeadCallResult(int status, long contentLength) {
        super(status, contentLength);
    }

    public AHCHeadCallResult(CallFailure fail) {
        super(fail);
    }

    public static AHCHeadCallResult notFound() {
        return new AHCHeadCallResult(404, -1);
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
