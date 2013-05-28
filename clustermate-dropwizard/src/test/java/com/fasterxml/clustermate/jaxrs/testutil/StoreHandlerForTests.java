package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.DeferredOperationQueue;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

public class StoreHandlerForTests extends StoreHandler<TestKey, StoredEntry<TestKey>, ListItem>
{
    public final static CustomerId CUSTOMER_WITH_GROUPING = CustomerId.valueOf("GRPD");
    
    public StoreHandlerForTests(SharedServiceStuff stuff,
            Stores<TestKey, StoredEntry<TestKey>> stores,
            ClusterViewByServer cluster)
    {
        super(stuff, stores, cluster);
    }

    @Override
    protected DeferredOperationQueue<TestKey> constructDeletionQueue(SharedServiceStuff stuff) {
        // No deferred deletes yet?
        return null;
    }
    
    @Override
    protected FakeLastAccess _findLastAccessUpdateMethod(ServiceRequest request, TestKey key)
    {
        return (key.getCustomerId() == CUSTOMER_WITH_GROUPING)
                ? FakeLastAccess.GROUPED : FakeLastAccess.INDIVIDUAL;
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

    @Override
    protected void updateLastAccessedForDelete(ServiceRequest request, ServiceResponse response,
            TestKey key, long accessTime)
    {
        FakeLastAccess acc = _findLastAccessUpdateMethod(request, key);
        // TODO: if there was grouped method, might not want to delete...
        if (acc == FakeLastAccess.INDIVIDUAL) {
            _stores.getLastAccessStore().removeLastAccess(key, acc, accessTime);
        }
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
