package com.fasterxml.clustermate.jaxrs.leveldb;

import java.io.File;

import com.fasterxml.jackson.databind.node.ObjectNode;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.RawEntryConverter;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.jaxrs.common.StatsTestBase;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;

public class StatsTest extends StatsTestBase
{
    @Override protected String testPrefix() { return "stats-leveldb"; }

    @Override
    protected StoreBackend createBackend(ServiceConfig config, File fileDir) {
        return LevelDBTestHelper.createLevelDBBackend(config, fileDir);
    }

    @Override
    protected NodeStateStore<IpAndPort, ActiveNodeState> createNodeStateStore(ServiceConfig config,
            RawEntryConverter<IpAndPort> keyConv, RawEntryConverter<ActiveNodeState> valueConv) {
        return LevelDBTestHelper.createLevelDBNodeStateStore(config, keyConv, valueConv);
    }

    @Override
    protected void _verifyStatsJson(ObjectNode json)
    {
        // Nothing much available unfortunately...
        _verifyExistenceOf(json, "stats");
    }
}
