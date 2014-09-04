package com.fasterxml.clustermate.service.remote;

import java.util.*;

import com.fasterxml.clustermate.api.NodeState;

/**
 * Container for information on nodes of remote cluster.
 */
public class RemoteCluster
{
    /**
     * Timestamp of when this representation was created. Used for keeping simple
     * time-to-live to force re-bootstrapping to ensure that changes to remove cluster
     * setup are eventually reflected in our view, even if other mechanisms fail.
     */
    protected final long _validUntil;
    
    protected final NodeState _localState;
    
    /**
     * Ordered set of remote nodes whose key ranges overlap with that of local node;
     * ordering is from primary node (first) to less preferable (backup) nodes.
     */
    protected final List<RemoteClusterNode> _overlappingPeers;

    public RemoteCluster(long validUntil, NodeState localState, List<RemoteClusterNode> peers)
    {
        _localState = localState;
        _validUntil = validUntil;
        _overlappingPeers = peers;
    }

    public boolean isStillValid(long currentTime) {
        return (currentTime <= _validUntil);
    }

    public NodeState getLocalState() {
        return _localState;
    }
    
    public List<RemoteClusterNode> getRemotePeers() {
        return _overlappingPeers;
    }

    public List<NodeState> asNodeStates() {
        ArrayList<NodeState> result = new ArrayList<NodeState>();
        for (RemoteClusterNode r : _overlappingPeers) {
            result.add(r.asNodeState(_localState));
        }
        return result;
    }
    
    @Override
    public String toString() {
        return "[RemoteCluster, overlappingPeers = "+_overlappingPeers+"]";
    }
}
