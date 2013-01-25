package com.fasterxml.clustermate.client.ahc;

import java.util.List;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.CallFailure;
import com.fasterxml.clustermate.client.call.ListCallResult;
import com.ning.http.client.HttpResponseHeaders;

public class AHCEntryListResult<T> extends ListCallResult<T>
{
    protected HttpResponseHeaders _headers;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public AHCEntryListResult(List<T> result) {
        super(result);
    }

    public AHCEntryListResult(CallFailure fail) {
        super(fail);
    }

    public AHCEntryListResult(int failCode) {
        super(failCode);
    }
    
    public static <T> AHCEntryListResult<T> notFound() {
        return new AHCEntryListResult<T>(ClusterMateConstants.HTTP_STATUS_NOT_FOUND);
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
