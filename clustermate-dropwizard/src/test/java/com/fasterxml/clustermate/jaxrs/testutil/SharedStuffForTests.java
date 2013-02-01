package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.file.FileManager;

public class SharedStuffForTests extends SharedServiceStuff
{
    private final ServiceConfigForTests _serviceConfig;

    private final StoredEntryConverter<TestKey, StoredEntry<TestKey>,ListItem> _entryConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public SharedStuffForTests(ServiceConfigForTests config, TimeMaster timeMaster,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>, ListItem> entryConverter,
            FileManager fileManager)
    {
        super(timeMaster, fileManager, config.getServicePathStrategy());
        _serviceConfig = config;
        _entryConverter = entryConverter;
    }

    /*
    /**********************************************************************
    /* Basic config access
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ServiceConfig> C getServiceConfig() {
        return (C) _serviceConfig;
    }

    @Override
    public StoreConfig getStoreConfig() {
     return _serviceConfig.storeConfig;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StoredEntryConverter<TestKey, StoredEntry<TestKey>,ListItem> getEntryConverter() {
        return _entryConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public EntryKeyConverter<TestKey> getKeyConverter() {
        return _entryConverter.keyConverter();
    }
}
