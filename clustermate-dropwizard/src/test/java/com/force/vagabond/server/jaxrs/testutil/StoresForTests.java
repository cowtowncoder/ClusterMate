package com.force.vagabond.server.jaxrs.testutil;

import java.io.File;

import com.sleepycat.je.Environment;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.clustermate.service.bdb.LastAccessStore;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.store.StoresImpl;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;

public class StoresForTests extends StoresImpl<TestKey, StoredEntry<TestKey>>
{
    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>> entryFactory, StorableStore entryStore) {
        this(config, timeMaster, jsonMapper,
                entryFactory, entryStore, null);
    }

    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>> entryConverter,
            StorableStore entryStore, File bdbEnvRoot)
    {
        super(config, timeMaster, jsonMapper, entryConverter, entryStore, bdbEnvRoot);
    }

    @Override
    protected LastAccessStore<TestKey, StoredEntry<TestKey>> buildAccessStore(Environment env) {
        return new LastAccessStoreForTests(env, _entryConverter);
    }
}
