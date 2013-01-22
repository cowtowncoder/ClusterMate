package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.storemate.client.CallFailure;
import com.fasterxml.storemate.client.call.EntryListResult;
import com.ning.http.client.HttpResponseHeaders;

public class AHCEntryListResult<T> extends EntryListResult<T>
{
    protected HttpResponseHeaders _headers;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public AHCEntryListResult(int status, T result) {
        super(status, result);
    }

    public AHCEntryListResult(CallFailure fail) {
        super(fail);
    }

    public static <T> AHCEntryListResult<T> notFound() {
        return new AHCEntryListResult<T>(ClusterMateConstants.HTTP_STATUS_NOT_FOUND, null);
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
