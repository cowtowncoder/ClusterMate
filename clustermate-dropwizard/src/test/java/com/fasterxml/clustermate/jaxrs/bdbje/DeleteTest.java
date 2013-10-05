package com.fasterxml.clustermate.jaxrs.bdbje;

import java.io.File;

import com.fasterxml.clustermate.jaxrs.common.DeleteTestBase;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class DeleteTest extends DeleteTestBase
{
    protected StoreBackend createBackend(ServiceConfig config, File fileDir) {
        return BDBTestHelper.createBDBJEBackend(config, fileDir);
    }
}
