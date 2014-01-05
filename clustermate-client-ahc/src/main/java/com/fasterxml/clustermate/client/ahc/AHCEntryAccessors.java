package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.EntryAccessors;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.ContentDeleter;
import com.fasterxml.clustermate.client.call.ContentGetter;
import com.fasterxml.clustermate.client.call.ContentHeader;
import com.fasterxml.clustermate.client.call.ContentPutter;
import com.fasterxml.clustermate.client.call.EntryLister;

import com.ning.http.client.AsyncHttpClient;

public class AHCEntryAccessors<K extends EntryKey, P extends Enum<P>>
    implements EntryAccessors<K>
{
    protected final StoreClientConfig<K,?> _storeConfig;
    protected final AsyncHttpClient _ahc;

    protected final P _singleEntryEndpoint;
    protected final P _entryListEndpoint;
    
    public AHCEntryAccessors(StoreClientConfig<K,?> storeConfig,
            P singleEndpoint, P listEndpoint,
            AsyncHttpClient ahc)
    {
        _storeConfig = storeConfig;
        _singleEntryEndpoint = singleEndpoint;
        _entryListEndpoint = listEndpoint;
        _ahc = ahc;
    }

    @Override
    public ContentPutter<K> entryPutter(ClusterServerNode server) {
        return new AHCContentPutter<K,P>(_storeConfig, _singleEntryEndpoint, _ahc, server);
    }

    @Override
    public ContentGetter<K> entryGetter(ClusterServerNode server) {
        return new AHCContentGetter<K,P>(_storeConfig, _singleEntryEndpoint, _ahc, server);
    }

    @Override
    public ContentHeader<K> entryHeader(ClusterServerNode server) {
        return new AHCContentHeader<K,P>(_storeConfig, _singleEntryEndpoint, _ahc, server);
    }

    @Override
    public ContentDeleter<K> entryDeleter(ClusterServerNode server) {
        return new AHCContentDeleter<K,P>(_storeConfig, _singleEntryEndpoint, _ahc, server);
    }

    @Override
    public EntryLister<K> entryLister(ClusterServerNode server) {
        return new AHCEntryLister<K,P>(_storeConfig, _entryListEndpoint, _ahc, server);
    }
}
