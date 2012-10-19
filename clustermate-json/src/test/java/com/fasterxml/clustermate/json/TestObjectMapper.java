package com.fasterxml.clustermate.json;

import com.fasterxml.clustermate.json.ClusterMateObjectMapper;
import com.fasterxml.storemate.api.ClusterStatusResponse;
import com.fasterxml.storemate.api.NodeState;

/**
 * Simple unit tests to verify that we can map basic JSON data
 * as expected. This verifies both basic definitions of types
 * we care about and correct functioning of mr Bean module
 * used to materialize abstract types.
 */
public class TestObjectMapper extends JsonTestBase
{
    public void testNodeState() throws Exception
    {
        ClusterMateObjectMapper mapper = new ClusterMateObjectMapper();
        NodeState state = mapper.readValue("{}", NodeState.class);
        assertNotNull(state);
    }

    public void testClusterStatusResponse() throws Exception
    {
        ClusterMateObjectMapper mapper = new ClusterMateObjectMapper();
        ClusterStatusResponse resp = mapper.readValue("{\"local\":{}}",
                ClusterStatusResponse.class);
        assertNotNull(resp);
        assertNotNull(resp.local);
    }
}
