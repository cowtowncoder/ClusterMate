package com.fasterxml.clustermate.json;

import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.clustermate.json.ClusterMateObjectMapper;

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
        ClusterStatusMessage resp = mapper.readValue("{\"local\":{}}",
                ClusterStatusMessage.class);
        assertNotNull(resp);
        assertNotNull(resp.local);
    }
}
