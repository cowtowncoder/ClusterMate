package com.fasterxml.clustermate.jaxrs.bdbje;

import java.io.File;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.storemate.backend.bdbje.BDBJEBuilder;
import com.fasterxml.storemate.backend.bdbje.BDBJEConfig;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.RawEntryConverter;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.state.NodeStateStore;

public class BDBTestHelper
{
    /**
     * This compile-time flag is here to remind that tests use (or not)
     * BDB-JE transctions. This should make no difference under normal test
     * conditions, since we don't do anything to cause problems. But
     * at higher level we may experience other issues with shutdown.
     */
    protected final static boolean USE_TRANSACTIONS = true;

    public static StoreBackend createBDBJEBackend(ServiceConfig config, File fileDir)
    {
        BDBJEConfig bdbConfig = new BDBJEConfig();
        bdbConfig.dataRoot = new File(fileDir.getParent(), "test-bdb");
        bdbConfig.useTransactions = USE_TRANSACTIONS;
        BDBJEBuilder b = new BDBJEBuilder(config.storeConfig, bdbConfig);
        return b.buildCreateAndInit();
    }

    public static NodeStateStore<IpAndPort, ActiveNodeState> createBDBNodeStateStore(ServiceConfig config,
            RawEntryConverter<IpAndPort> keyConv, RawEntryConverter<ActiveNodeState> valueConv) {
        BDBJEConfig bdbConfig = new BDBJEConfig();
        BDBJEBuilder b = new BDBJEBuilder(config.storeConfig, bdbConfig);
        return b.buildNodeStateStore(config.metadataDirectory, keyConv, valueConv);
    }
}
