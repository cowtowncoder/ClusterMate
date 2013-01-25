package com.fasterxml.clustermate.client;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.call.*;

/**
 * "Factory interface" that contains factory methods needed for constructing
 * accessors for CRUD operations on stored entries.
 */
public interface EntryAccessors<K extends EntryKey>
{
    public abstract ContentPutter<K> entryPutter(ClusterServerNode server);

    public abstract ContentGetter<K> entryGetter(ClusterServerNode server);

    public abstract ContentHeader<K> entryHeader(ClusterServerNode server);

    public abstract ContentDeleter<K> entryDeleter(ClusterServerNode server);

    public abstract EntryLister<K> entryLister(ClusterServerNode server);
}
