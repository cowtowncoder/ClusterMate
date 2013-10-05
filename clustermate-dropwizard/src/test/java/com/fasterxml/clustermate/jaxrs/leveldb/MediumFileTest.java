package com.fasterxml.clustermate.jaxrs.leveldb;

import java.io.File;

import com.fasterxml.clustermate.jaxrs.common.MediumFileTestBase;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class MediumFileTest extends MediumFileTestBase
{
    protected StoreBackend createBackend(ServiceConfig config, File fileDir) {
        return LevelDBTestHelper.createLevelDBBackend(config, fileDir);
    }
}
