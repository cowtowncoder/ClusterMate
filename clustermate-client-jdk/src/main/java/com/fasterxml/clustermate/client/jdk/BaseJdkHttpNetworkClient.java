package com.fasterxml.clustermate.client.jdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public abstract class BaseJdkHttpNetworkClient<
    K extends EntryKey,
    CONFIG extends StoreClientConfig<K,CONFIG>
>
    extends NetworkClient<K>
{
    protected final ObjectMapper _mapper;

    protected final CONFIG _config;
    
    /**
     * The usual constructor to call; configures AHC using standard
     * settings.
     */
    protected BaseJdkHttpNetworkClient(CONFIG config)
    {
        _config = config;
        _mapper = config.getJsonMapper();
    }

    /*
    /**********************************************************************
    /* Standard factory methods
    /**********************************************************************
     */

    @Override
    public JdkHttpClientPathBuilder pathBuilder(IpAndPort server) {
        return new JdkHttpClientPathBuilder(server);
    }
    
    @Override
    public void shutdown() {
        // nothing to do here
    }
    
    @Override
    public EntryAccessors<K> getEntryAccessors() {
        return new JdkHttpEntryAccessors<K>(_config);
    }

    @Override
    public EntryKeyConverter<K> getKeyConverter() {
        return _config.getKeyConverter();
    }
}
