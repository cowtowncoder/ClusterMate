package com.fasterxml.clustermate.client.cluster;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.NodesForKey;

/**
 * Helper class we use to encapsulate details of finding ordered sequence
 * of servers to contact, to locate entry with given key.
 */
public final class HashRouter<K extends EntryKey>
{
    private final KeySpace _keyspace;
    
    private final EntryKeyConverter<K> _keyConverter;

    /**
     * Monotonically increasing counter we use for lazily constructing
     * and invalidating routing information, mapping from key hashes
     * to {@link NodesForKey} objects.
     */
    private final AtomicInteger _version = new AtomicInteger(1);

    private final AtomicReferenceArray<NodesForKey> _routing;

    /**
     * Since we will need to iterate over server nodes, let's use pre-calculated
     * array. Parent will update it, we just need to access it in thread-safe
     * manner.
     */
    private AtomicReference<ClusterServerNode[]> _states = new AtomicReference<ClusterServerNode[]>(
            new ClusterServerNode[0]);
    
    public HashRouter(KeySpace keyspace, EntryKeyConverter<K> keyConverter,
            AtomicReference<ClusterServerNode[]> states)
    {
        _keyspace = keyspace;
        _keyConverter = keyConverter;
        _routing = new AtomicReferenceArray<NodesForKey>(keyspace.getLength());
        _states = states;
    }

    public void invalidateRouting() {
        _version.addAndGet(1);
    }
    
    public NodesForKey getNodesFor(K key)
    {
        int fullHash = _keyConverter.routingHashFor(key);
        KeyHash hash = new KeyHash(fullHash, _keyspace.getLength());
        int currVersion = _version.get();
        int modulo = hash.getModuloHash();
        NodesForKey nodes = _routing.get(modulo);
        // fast (and common) case: pre-calculated, valid info exists:
        if (nodes != null && nodes.version() == currVersion) {
            return nodes;
        }
        NodesForKey newNodes = _calculateNodes(currVersion, hash);
        _routing.compareAndSet(modulo, nodes, newNodes);
        return newNodes;
    }

    protected NodesForKey _calculateNodes(int version, KeyHash keyHash) {
        return _calculateNodes(version, keyHash, _states.get());
    }

    // separate method for testing
    protected NodesForKey _calculateNodes(int version, KeyHash keyHash,
            ClusterServerNode[] allNodes)
    {
        final int allCount = allNodes.length;
        // First: simply collect all applicable nodes:
        ArrayList<ClusterServerNode> appl = new ArrayList<ClusterServerNode>();
        for (int i = 0; i < allCount; ++i) {
            ClusterServerNode state = allNodes[i];
            if (state.getTotalRange().contains(keyHash)) {
                appl.add(state);
            }
        }
        return _sortNodes(version, keyHash, appl);
    }

    protected NodesForKey _sortNodes(int version, KeyHash keyHash,
            Collection<ClusterServerNode> appl)
    {
        // edge case: no matching
        if (appl.isEmpty()) {
            return NodesForKey.empty(version);
        }
        // otherwise need to sort
        ClusterServerNodeImpl[] matching = appl.toArray(new ClusterServerNodeImpl[appl.size()]);
        Arrays.sort(matching, 0, appl.size(), new NodePriorityComparator(keyHash));
        return new NodesForKey(version, matching);
    }

    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */
    
    /**
     * Comparator that orders server in decreasing priority, that is, starts with
     * the closest enabled match, ending with disabled entries.
     */
    private final static class NodePriorityComparator implements Comparator<ClusterServerNodeImpl>
    {
        private final KeyHash _keyHash;
    
        public NodePriorityComparator(KeyHash keyHash) {
            _keyHash = keyHash;
        }
        
        @Override
        public int compare(ClusterServerNodeImpl node1, ClusterServerNodeImpl node2)
        {
            return node1.calculateSortingDistance(_keyHash) - node2.calculateSortingDistance(_keyHash);
        }
    }
}
