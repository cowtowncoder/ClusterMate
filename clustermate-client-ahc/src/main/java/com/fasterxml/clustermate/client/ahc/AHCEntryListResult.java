package com.fasterxml.clustermate.client.ahc;

import com.ning.http.client.HttpResponseHeaders;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.ListCallResult;

public class AHCEntryListResult<T> extends ListCallResult<T>
{
    protected HttpResponseHeaders _headers;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public AHCEntryListResult(ClusterServerNode server, ListResponse<T> resp) {
        super(server, resp);
    }

    public AHCEntryListResult(CallFailure fail) {
        super(fail);
    }

    public AHCEntryListResult(ClusterServerNode server, int failCode) {
        super(server, failCode);
    }
    
    public static <T> AHCEntryListResult<T> notFound(ClusterServerNode server) {
        return new AHCEntryListResult<T>(server, ClusterMateConstants.HTTP_STATUS_NOT_FOUND);
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
