package com.fasterxml.clustermate.service.lastaccess;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.lastaccess.LastAccessConfig;
import com.fasterxml.storemate.store.lastaccess.LastAccessConverter;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

public abstract class LastAccessStoreHelper
{
    public static <K extends EntryKey, 
        E extends StoredEntry<K>,
        ACC extends LastAccessUpdateMethod
    >
    LastAccessStore<K,E,ACC> defaultLastAccessStore(StoreBackendBuilder<?> backendBuilder,
            SharedServiceStuff stuff, File metadataDir,
            LastAccessConfig config,
            LastAccessConverter<K, E, ACC> lastAccessedConverter)
    {
        /*
        public BDBLastAccessStoreImpl(LastAccessConfig config,
                LastAccessConverter<K, E, ACC> lastAccessedConverter,
                Environment env
        
        final ObjectMapper mapper = stuff.jsonMapper();
        */
        /*
        NodeStateStore<IpAndPort, ActiveNodeState> nodeStates =
                backendBuilder.buildNodeStateStore(metadataDir,
                        new JacksonBasedConverter<IpAndPort>(mapper, IpAndPort.class),
                        new JacksonBasedConverter<ActiveNodeState>(mapper, ActiveNodeState.class));
        return nodeStates;
        */
        return null;
    }
}
