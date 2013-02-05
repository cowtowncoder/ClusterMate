package com.fasterxml.clustermate.jaxrs.testutil;

import java.io.File;

import com.sleepycat.je.Environment;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;

import com.fasterxml.clustermate.service.bdb.LastAccessStore;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.store.StoresImpl;

public class StoresForTests extends StoresImpl<TestKey, StoredEntry<TestKey>>
{
    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>,FakeFullListItem> entryFactory,
            StorableStore entryStore) {
        this(config, timeMaster, jsonMapper,
                entryFactory, entryStore, null);
    }

    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>,FakeFullListItem> entryConverter,
            StorableStore entryStore, File bdbEnvRoot)
    {
        super(config, timeMaster, jsonMapper, entryConverter, entryStore, bdbEnvRoot);
    }

    @Override
    protected LastAccessStore<TestKey, StoredEntry<TestKey>> buildAccessStore(Environment env) {
        return new LastAccessStoreForTests(env, _entryConverter);
    }
}
