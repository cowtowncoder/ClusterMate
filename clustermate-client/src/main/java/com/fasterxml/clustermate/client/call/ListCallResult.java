package com.fasterxml.clustermate.client.call;

import java.util.List;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.storemate.shared.StorableKey;

public abstract class ListCallResult<T>
    extends ReadCallResult<ListResponse<T>>
{
    protected final StorableKey _lastSeen;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */

    public ListCallResult(ListResponse<T> resp)
    {
        super(resp);
        _lastSeen = resp.lastSeen;
    }

    public ListCallResult(CallFailure fail)
    {
        super(fail);
        _lastSeen = null;
    }

    public ListCallResult(int statusCode)
    {
        super(statusCode);
        _lastSeen = null;
    }
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */
    
    public List<T> getItems() {
        return (_result == null) ? null : _result.items;
    }

    public StorableKey getLastSeen() { return _lastSeen; }
}
