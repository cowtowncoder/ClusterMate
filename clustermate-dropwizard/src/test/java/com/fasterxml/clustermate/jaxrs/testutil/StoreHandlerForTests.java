package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

public class StoreHandlerForTests extends StoreHandler<TestKey, StoredEntry<TestKey>, ListItem>
{
    public StoreHandlerForTests(SharedServiceStuff stuff,
            Stores<TestKey, StoredEntry<TestKey>> stores,
            ClusterViewByServer cluster)
    {
        super(stuff, stores, cluster);
    }
    
    @Override
    protected LastAccessUpdateMethod _findLastAccessUpdateMethod(TestKey key)
    {
        return key.hasGroupId() ? FakeLastAccess.GROUPED
                : FakeLastAccess.INDIVIDUAL;
    }
        
    @Override
    protected void updateLastAccessedForGet(ServiceRequest request, ServiceResponse response,
            StoredEntry<TestKey> entry, long accessTime)
    {
        _updateLastAccessed(entry.getKey(), entry, accessTime);
    }

    @Override
    protected void updateLastAccessedForHead(ServiceRequest request, ServiceResponse response,
            StoredEntry<TestKey> entry, long accessTime)
    {
        _updateLastAccessed(entry.getKey(), entry, accessTime);
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    private void _updateLastAccessed(TestKey key, StoredEntry<TestKey> entry, long accessTime)
    {
        _stores.getLastAccessStore().updateLastAccess(entry, accessTime);
    }
}
