package com.fasterxml.clustermate.jaxrs.leveldb;

import java.io.File;

import com.fasterxml.storemate.store.backend.StoreBackend;

import com.fasterxml.clustermate.jaxrs.common.CompressionTestBase;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;

public class CompressionTest extends CompressionTestBase
{
    @Override protected String testPrefix() { return "compress-leveldb"; }

    @Override
    protected StoreBackend createBackend(ServiceConfig config, File fileDir) {
        return LevelDBTestHelper.createLevelDBBackend(config, fileDir);
    }
}
