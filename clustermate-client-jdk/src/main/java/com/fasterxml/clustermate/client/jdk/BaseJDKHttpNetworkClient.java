package com.fasterxml.clustermate.client.jdk;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.IpAndPort;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public abstract class BaseJDKHttpNetworkClient<
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
    protected BaseJDKHttpNetworkClient(CONFIG config)
    {
        _config = config;
        _mapper = config.getJsonMapper();
        /*
            .setConnectionTimeoutInMs((int)config.getCallConfig().getConnectTimeoutMsecs())
            .setMaximumConnectionsPerHost(5) // default of 2 is too low
            .setMaximumConnectionsTotal(30) // and 10 is bit skimpy too
            */
    }

    /*
    /**********************************************************************
    /* Standard factory methods
    /**********************************************************************
     */

    @Override
    public RequestPathBuilder pathBuilder(IpAndPort server)
    {
        return new JdkHttpClientPathBuilder(server);
    }
    
    @Override
    public void shutdown() {
        // nothing to do here
    }
    
    @Override
    public EntryAccessors<K> getEntryAccessors() {
        return new JDKHttpEntryAccessors<K>(_config);
    }

    @Override
    public EntryKeyConverter<K> getKeyConverter() {
        return _config.getKeyConverter();
    }
}
