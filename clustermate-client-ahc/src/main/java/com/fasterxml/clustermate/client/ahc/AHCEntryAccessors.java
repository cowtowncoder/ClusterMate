package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.EntryAccessors;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.ContentDeleter;
import com.fasterxml.clustermate.client.call.ContentGetter;
import com.fasterxml.clustermate.client.call.ContentHeader;
import com.fasterxml.clustermate.client.call.ContentPutter;
import com.fasterxml.clustermate.client.call.EntryInspector;
import com.fasterxml.clustermate.client.call.EntryLister;
import com.ning.http.client.AsyncHttpClient;

public class AHCEntryAccessors<K extends EntryKey>
    implements EntryAccessors<K>
{
    protected final StoreClientConfig<K,?> _storeConfig;
    protected final AsyncHttpClient _ahc;
    
    public AHCEntryAccessors(StoreClientConfig<K,?> storeConfig,
            AsyncHttpClient ahc)
    {
        _storeConfig = storeConfig;
        _ahc = ahc;
    }

    @Override
    public ContentPutter<K> entryPutter(ClusterServerNode server) {
        return new AHCContentPutter<K>(_storeConfig, _ahc, server);
    }

    @Override
    public ContentGetter<K> entryGetter(ClusterServerNode server) {
        return new AHCContentGetter<K>(_storeConfig, _ahc, server);
    }

    @Override
    public ContentHeader<K> entryHeader(ClusterServerNode server) {
        return new AHCContentHeader<K>(_storeConfig, _ahc, server);
    }

    @Override
    public ContentDeleter<K> entryDeleter(ClusterServerNode server) {
        return new AHCContentDeleter<K>(_storeConfig, _ahc, server);
    }

    @Override
    public EntryLister<K> entryLister(ClusterServerNode server) {
        return new AHCEntryLister<K>(_storeConfig, _ahc, server);
    }

    @Override
    public EntryInspector<K> entryInspector(ClusterServerNode server) {
        return new AHCEntryInspector<K>(_storeConfig, _ahc, server);
    }
}
