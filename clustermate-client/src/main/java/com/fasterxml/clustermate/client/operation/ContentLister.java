package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.storemate.shared.EntryKey;

/**
 * Value class that is used as result type for content list operation.
 * Unlike simple result classes like {@link GetOperationResult}, no calls
 * are yet made when this object is constructed; rather, it is returned
 * to be used for incrementally accessing contents to list.
 * This is necessary as list operations may return large number of entries,
 * and each individual operation can only return up to certain number of
 * entries.
 */
public class ContentLister<K extends EntryKey>
{
    protected final StoreClientConfig<K,?> _clientConfig;
    
    /**
     * Prefix of entries to list.
     */
    protected final K _prefix;

    public ContentLister(StoreClientConfig<K,?> config, K prefix) {
        _clientConfig = config;
        _prefix = prefix;
    }
}
