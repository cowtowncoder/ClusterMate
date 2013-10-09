package com.fasterxml.clustermate.jaxrs.bdbje;

import java.io.File;

import com.fasterxml.clustermate.jaxrs.common.SmallFileTestBase;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class SmallFileTest extends SmallFileTestBase
{
    @Override protected String testPrefix() { return "small-bdb"; }

    @Override
    protected StoreBackend createBackend(ServiceConfig config, File fileDir) {
        return BDBTestHelper.createBDBJEBackend(config, fileDir);
    }
}
