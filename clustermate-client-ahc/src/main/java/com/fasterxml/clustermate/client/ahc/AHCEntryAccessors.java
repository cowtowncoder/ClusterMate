package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.cluster.EntryAccessors;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.client.call.ContentDeleter;
import com.fasterxml.storemate.client.call.ContentGetter;
import com.fasterxml.storemate.client.call.ContentHeader;
import com.fasterxml.storemate.client.call.ContentPutter;
import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.EntryKeyConverter;

import com.ning.http.client.AsyncHttpClient;

public class AHCEntryAccessors<K extends EntryKey> implements EntryAccessors<K>
{
    protected final AsyncHttpClient _ahc;
    protected final ObjectMapper _mapper;
    protected final EntryKeyConverter<K> _keyConverter;
    
    public AHCEntryAccessors(AsyncHttpClient ahc,
            ObjectMapper mapper, EntryKeyConverter<K> keyConverter)
    {
        _ahc = ahc;
        _mapper = mapper;
        _keyConverter = keyConverter;
    }
    
    @Override
    public ContentPutter<K> entryPutter(ClusterServerNode server) {
        return new AHCContentPutter<K>(_ahc, _mapper, _keyConverter, server);
    }

    @Override
    public ContentGetter<K> entryGetter(ClusterServerNode server) {
        return new AHCContentGetter<K>(_ahc, _mapper, server);
    }

    @Override
    public ContentHeader<K> entryHeader(ClusterServerNode server) {
        return new AHCContentHeader<K>(_ahc, server);
    }

    @Override
    public ContentDeleter<K> entryDeleter(ClusterServerNode server) {
        return new AHCContentDeleter<K>(_ahc, _mapper, server);
    }

}
