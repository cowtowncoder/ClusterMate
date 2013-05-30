package com.fasterxml.clustermate.service.store;

import com.fasterxml.storemate.shared.StorableKey;

public class DeferredDeletion
{
    public final long insertTime;
    public final StorableKey key;
    
    public DeferredDeletion(long insertTime, StorableKey key)
    {
        this.insertTime = insertTime;
        this.key = key;
    }
}
