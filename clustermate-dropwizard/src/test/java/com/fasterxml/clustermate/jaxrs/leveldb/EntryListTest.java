package com.fasterxml.clustermate.jaxrs.leveldb;

import java.io.File;

import com.fasterxml.clustermate.jaxrs.EntryListTestBase;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class EntryListTest extends EntryListTestBase
{
    protected StoreBackend createBackend(ServiceConfig config, File fileDir) {
        return LevelDBTestHelper.createLevelDBBackend(config, fileDir);
    }
}
