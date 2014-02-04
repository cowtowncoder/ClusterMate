package com.fasterxml.clustermate.client.call;

import java.util.List;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.storemate.shared.StorableKey;

public abstract class ListCallResult<T>
    extends CallResult
{
    protected final List<T> _items;
    
    protected final StorableKey _lastSeen;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public ListCallResult(ListResponse<T> resp)
    {
        super(ClusterMateConstants.HTTP_STATUS_OK);
        _items = resp.items;
        _lastSeen = resp.lastSeen;
    }

    public ListCallResult(CallFailure fail)
    {
        super(fail);
        _items = null;
        _lastSeen = null;
    }

    public ListCallResult(int statusCode)
    {
        super(statusCode);
        _items = null;
        _lastSeen = null;
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

    public StorableKey getLastSeen() { return _lastSeen; }
}
