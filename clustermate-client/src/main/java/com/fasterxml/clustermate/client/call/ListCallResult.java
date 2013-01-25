package com.fasterxml.clustermate.client.call;

import java.util.List;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.client.CallFailure;

public abstract class ListCallResult<T>
    extends CallResult
{
    protected final List<T> _items;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public ListCallResult(ListResponse<T> resp)
    {
        super(ClusterMateConstants.HTTP_STATUS_OK);
        _items = resp.items;
    }

    public ListCallResult(CallFailure fail)
    {
        super(fail);
        _items = null;
    }

    public ListCallResult(int statusCode)
    {
        super(statusCode);
        _items = null;
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
    
    public List<T> getItems() { return _items; }
}
