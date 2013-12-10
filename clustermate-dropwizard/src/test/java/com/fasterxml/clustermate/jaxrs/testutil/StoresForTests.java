package com.fasterxml.clustermate.jaxrs.testutil;

import java.io.File;

import com.sleepycat.je.Environment;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessConfig;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.service.LastAccessStore;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.store.StoresImpl;

public class StoresForTests extends StoresImpl<TestKey, StoredEntry<TestKey>>
{
    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>,FakeFullListItem> entryFactory,
            StorableStore entryStore, NodeStateStore<IpAndPort, ActiveNodeState> nodeStates) {
        this(config, timeMaster, jsonMapper,
                entryFactory, entryStore, nodeStates, null);
    }

    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>,FakeFullListItem> entryConverter,
            StorableStore entryStore, NodeStateStore<IpAndPort, ActiveNodeState> nodeStates,
            File dataStoreRoot)
    {
        super(config, timeMaster, jsonMapper, entryConverter, entryStore, nodeStates, dataStoreRoot);
    }

    @Override
    protected LastAccessStore<TestKey, StoredEntry<TestKey>> buildAccessStore(Environment env,
            LastAccessConfig config) {
        return new LastAccessStoreForTests(env, _entryConverter, config);
    }
}
