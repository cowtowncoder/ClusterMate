package com.fasterxml.clustermate.service.state;

import java.io.File;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.state.NodeStateStore;

public abstract class NodeStateStoreHelper
{
    public static NodeStateStore<IpAndPort, ActiveNodeState> defaultNodeStateStore(StoreBackendBuilder<?> backendBuilder,
            SharedServiceStuff stuff, File metadataDir)
    {
        final ObjectMapper mapper = stuff.jsonMapper();
        NodeStateStore<IpAndPort, ActiveNodeState> nodeStates =
                backendBuilder.buildNodeStateStore(metadataDir,
                        new JacksonBasedConverter<IpAndPort>(mapper, IpAndPort.class),
                        new JacksonBasedConverter<ActiveNodeState>(mapper, ActiveNodeState.class));
        return nodeStates;
    }
}
