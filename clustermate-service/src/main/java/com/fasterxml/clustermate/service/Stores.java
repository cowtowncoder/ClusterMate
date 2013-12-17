package com.fasterxml.clustermate.service;

import java.io.File;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.storemate.store.state.NodeStateStore;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Basic abstraction used for handling references to core stores
 * that a clustered service needs.
 */
public abstract class Stores<K extends EntryKey, E extends StoredEntry<K>>
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    public abstract boolean isActive();

    public abstract String getInitProblem();

    public abstract File getNodeDirectory();

    public abstract StoredEntryConverter<K,E,?> getEntryConverter();
    
    public abstract StorableStore getEntryStore();
    public abstract NodeStateStore<IpAndPort, ActiveNodeState> getNodeStore();
    public abstract LastAccessStore<K,E,LastAccessUpdateMethod> getLastAccessStore();
}
