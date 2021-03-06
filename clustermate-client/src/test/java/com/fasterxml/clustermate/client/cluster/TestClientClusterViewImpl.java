package com.fasterxml.clustermate.client.cluster;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.client.NodesForKey;
import com.fasterxml.clustermate.client.cluster.ClusterServerNodeImpl;
import com.fasterxml.clustermate.client.cluster.ClusterViewByClientImpl;

public class TestClientClusterViewImpl extends ClientTestBase
{
    protected final KeySpace DEFAULT_SPACE = new KeySpace(360);

    protected final KeyRange RANGE1 = DEFAULT_SPACE.range(0, 120); // 0 - 119
    protected final KeyRange RANGE2 = DEFAULT_SPACE.range(90, 60); // 90 - 149
    protected final KeyRange RANGE3 = DEFAULT_SPACE.range(300, 90); // 300 - 29

    protected final ClusterServerNodeImpl NODE1 = ClusterServerNodeImpl.forTesting(RANGE1);
    protected final ClusterServerNodeImpl NODE2 = ClusterServerNodeImpl.forTesting(RANGE2);
    protected final ClusterServerNodeImpl NODE3 = ClusterServerNodeImpl.forTesting(RANGE3);

    protected final ClusterServerNodeImpl[] allNodes = new ClusterServerNodeImpl[] { NODE1, NODE2, NODE3 };
    
    public void testSimpleDistanceCalc()
    {
        ClusterViewByClientImpl<EntryKey> view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);
        NodesForKey nodes;

        // first: test with all enabled
        
        // 2 ranges overlap; range2 closer to start
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(100), allNodes);
        assertEquals(2, nodes.size());
        assertEquals(90, nodes.node(0).getTotalRange().getStart());
        assertEquals(0, nodes.node(1).getTotalRange().getStart());

        // 2 here too
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(10), allNodes);
        assertEquals(2, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());
        assertEquals(300, nodes.node(1).getTotalRange().getStart());
        
        // nothing covers this:
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(180), allNodes);
        assertEquals(0, nodes.size());

        // and just one node these:
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(80), allNodes);
        assertEquals(1, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());

        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(350), allNodes);
        assertEquals(1, nodes.size());
        assertEquals(300, nodes.node(0).getTotalRange().getStart());
    }

    public void testDistanceCalcWithDisabled()
    {
        ClusterViewByClientImpl<EntryKey> view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);
        NodesForKey nodes;

        ClusterServerNodeImpl disabledNode2 = ClusterServerNodeImpl.forTesting(RANGE2);
        disabledNode2.updateDisabled(true);
        
        ClusterServerNodeImpl[] disabledStates = new ClusterServerNodeImpl[] { NODE1, disabledNode2, NODE3 };
        
        // 2 ranges overlap; range2 would be closer but is disabled
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(100), disabledStates);
        assertEquals(2, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());
        assertEquals(90, nodes.node(1).getTotalRange().getStart());

        // 2 here too; no change
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(10), disabledStates);
        assertEquals(2, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());
        assertEquals(300, nodes.node(1).getTotalRange().getStart());
        
        // nothing covers this either
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(180), allNodes);
        assertEquals(0, nodes.size());
    }

    public void testActiveVsPassive()
    {
        ClusterViewByClientImpl<EntryKey> view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);

        KeyRange range1a = DEFAULT_SPACE.range(30, 60); // 30 - 89
        KeyRange range1p = DEFAULT_SPACE.range(0, 120); // 0 - 119

        KeyRange range2a = DEFAULT_SPACE.range(60, 60); // 60 - 119
        KeyRange range2p = DEFAULT_SPACE.range(30, 120); // 30 - 149

        ClusterServerNodeImpl node1 = ClusterServerNodeImpl.forTesting(range1a, range1p);
        ClusterServerNodeImpl node2 = ClusterServerNodeImpl.forTesting(range2a, range2p);

        NodesForKey nodes;

        // overlap; both included...
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(30),
                new ClusterServerNodeImpl[] { node1, node2 });
        assertEquals(2, nodes.size());
        assertSame(node1, nodes.node(0));
        assertSame(node2, nodes.node(1));

        // changing order should not matter:
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(30),
                new ClusterServerNodeImpl[] { node2, node1 });
        assertEquals(2, nodes.size());
        assertSame(node1, nodes.node(0));
        assertSame(node2, nodes.node(1));
    }
    
    public void testFullCoverage()
    {
        ClusterViewByClientImpl<EntryKey> view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);
        assertEquals(210, view._getCoverage(allNodes));

        KeyRange extraRange = DEFAULT_SPACE.range(100, 200);
        ClusterServerNodeImpl extraNode = ClusterServerNodeImpl.forTesting(extraRange);

        ClusterServerNodeImpl[] moreNodes = new ClusterServerNodeImpl[] { NODE1, NODE2, NODE3, extraNode };
        assertEquals(360, view._getCoverage(moreNodes));
    }
}
