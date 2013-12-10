package com.fasterxml.clustermate.jaxrs.leveldb;

import java.io.File;

import com.fasterxml.storemate.backend.leveldb.LevelDBBuilder;
import com.fasterxml.storemate.backend.leveldb.LevelDBConfig;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.RawEntryConverter;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;

public class LevelDBTestHelper
{
    public static StoreBackend createLevelDBBackend(ServiceConfig serviceConfig, File fileDir)
    {
        LevelDBConfig dbConfig = new LevelDBConfig();
        dbConfig.dataRoot = new File(fileDir.getParent(), "test-leveldb");
        LevelDBBuilder b = new LevelDBBuilder(serviceConfig.storeConfig, dbConfig);
        return b.buildCreateAndInit();
    }

    public static NodeStateStore<IpAndPort, ActiveNodeState> createLevelDBNodeStateStore(ServiceConfig config,
            RawEntryConverter<IpAndPort> keyConv, RawEntryConverter<ActiveNodeState> valueConv) {
        LevelDBConfig dbConfig = new LevelDBConfig();
        LevelDBBuilder b = new LevelDBBuilder(config.storeConfig, dbConfig);
        return b.buildNodeStateStore(config.metadataDirectory, keyConv, valueConv);
    }
}
