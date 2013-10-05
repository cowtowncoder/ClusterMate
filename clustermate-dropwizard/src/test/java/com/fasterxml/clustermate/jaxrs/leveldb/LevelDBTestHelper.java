package com.fasterxml.clustermate.jaxrs.leveldb;

import java.io.File;

import com.fasterxml.storemate.backend.leveldb.LevelDBBuilder;
import com.fasterxml.storemate.backend.leveldb.LevelDBConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;

public class LevelDBTestHelper
{
    public static StoreBackend createLevelDBBackend(ServiceConfig serviceConfig, File fileDir)
    {
        LevelDBConfig dbConfig = new LevelDBConfig();
        dbConfig.dataRoot = new File(fileDir.getParent(), "test-leveldb");
        LevelDBBuilder b = new LevelDBBuilder(serviceConfig.storeConfig, dbConfig);
        return b.buildCreateAndInit();
    }
}
