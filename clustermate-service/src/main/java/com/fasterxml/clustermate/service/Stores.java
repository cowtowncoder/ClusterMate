package com.fasterxml.clustermate.service;

import java.io.File;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.store.StorableStore;

import com.fasterxml.clustermate.service.bdb.LastAccessStore;
import com.fasterxml.clustermate.service.bdb.NodeStateStore;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Basic abstraction used for handling references to core stores
 * that a clustered service needs.
 */
public abstract class Stores<K extends EntryKey, E extends StoredEntry<K>>
    implements StartAndStoppable
{
    public abstract boolean isActive();

    public abstract String getInitProblem();

    public abstract File getNodeDirectory();

    public abstract StoredEntryConverter<K,E> getEntryConverter();
    
    public abstract StorableStore getEntryStore();
    public abstract NodeStateStore getNodeStore();
    public abstract LastAccessStore<K,E> getLastAccessStore();

}
