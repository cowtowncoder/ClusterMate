package com.fasterxml.clustermate.client;

import com.fasterxml.clustermate.client.cluster.EntryAccessors;
import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.EntryKeyConverter;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.RequestPathBuilder;

/**
 * Factory abstraction used to separate details of physical network Client,
 * and logical functionality needed by higher-level client implementation.
 */
public abstract class NetworkClient<K extends EntryKey>
{
    public abstract RequestPathBuilder pathBuilder(IpAndPort server);

    /**
     * Method to call to shut down client implementation; called when
     * main client library is stopped.
     */
    public abstract void shutdown();

    /**
     * Accessor for factory method(s) for creating per-server accessor objects.
     */
    public abstract EntryAccessors<K> getEntryAccessors();

    public abstract EntryKeyConverter<K> getKeyConverter();
}
