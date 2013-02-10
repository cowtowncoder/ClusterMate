package com.fasterxml.clustermate.client.jdk;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.*;

public class JDKHttpEntryAccessors<K extends EntryKey> implements EntryAccessors<K>
{
    protected final StoreClientConfig<K,?> _storeConfig;
    
    public JDKHttpEntryAccessors(StoreClientConfig<K,?> storeConfig)
    {
        _storeConfig = storeConfig;
    }
    
    @Override
    public ContentPutter<K> entryPutter(ClusterServerNode server) {
        return new JdkHttpContentPutter<K>(_storeConfig, server);
    }

    @Override
    public ContentGetter<K> entryGetter(ClusterServerNode server) {
        return new JdkHttpContentGetter<K>(_storeConfig, server);
    }

    @Override
    public ContentHeader<K> entryHeader(ClusterServerNode server) {
        return new JdkHttpContentHeader<K>(_storeConfig, server);
    }

    @Override
    public ContentDeleter<K> entryDeleter(ClusterServerNode server) {
        return new JdkHttpContentDeleter<K>(_storeConfig, server);
    }

    @Override
    public EntryLister<K> entryLister(ClusterServerNode server) {
        return new JdkHttpEntryLister<K>(_storeConfig, server);
    }
}
