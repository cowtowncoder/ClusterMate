package com.fasterxml.clustermate.client.jdk;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.*;

public class JdkHttpEntryAccessors<K extends EntryKey, P extends Enum<P>>
    implements EntryAccessors<K>
{
    protected final StoreClientConfig<K,?> _storeConfig;

    protected final P _singleEntryEndpoint;
    protected final P _entryListEndpoint;

    public JdkHttpEntryAccessors(StoreClientConfig<K,?> storeConfig,
            P singleEndpoint, P listEndpoint)
    {
        _storeConfig = storeConfig;
        _singleEntryEndpoint = singleEndpoint;
        _entryListEndpoint = listEndpoint;
    }
    
    @Override
    public ContentPutter<K> entryPutter(ClusterServerNode server) {
        return new JdkHttpContentPutter<K,P>(_storeConfig, _singleEntryEndpoint, server);
    }

    @Override
    public ContentGetter<K> entryGetter(ClusterServerNode server) {
        return new JdkHttpContentGetter<K,P>(_storeConfig, _singleEntryEndpoint, server);
    }

    @Override
    public ContentHeader<K> entryHeader(ClusterServerNode server) {
        return new JdkHttpContentHeader<K,P>(_storeConfig, _singleEntryEndpoint, server);
    }

    @Override
    public ContentDeleter<K> entryDeleter(ClusterServerNode server) {
        return new JdkHttpContentDeleter<K,P>(_storeConfig, _singleEntryEndpoint, server);
    }

    @Override
    public EntryLister<K> entryLister(ClusterServerNode server) {
        return new JdkHttpEntryLister<K,P>(_storeConfig, _entryListEndpoint, server);
    }
}
