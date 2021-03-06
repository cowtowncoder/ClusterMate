package com.fasterxml.clustermate.api.msg;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.clustermate.api.NodeState;

/**
 * POJO used to exchange information on status of the cluster;
 * that is, settings for nodes that are known by a given server node.
 * These are exchanged by servers piggy-backed on sync list/pull
 * requests; and may be POSTed on startup and/or shutdown.
 */
public class ClusterStatusMessage
{
    /**
     * Status of the local node.
     */
    public NodeState local;

    /**
     * Set of "other" nodes in this cluster.
     */
    public Collection<NodeState> localPeers;

    /**
     * And optional set of one or more peers in remote cluster(s).
     */
    public Collection<NodeState> remotePeers;
    
    /**
     * Timestamp of time when this message was composed.
     */
    public long creationTime;

    /**
     * Timestamp of last update to aggregated cluster information by the
     * serving server node.
     * May be used for diagnostic purposes, or possibly optimizing access.
     */
    public long clusterLastUpdated;
    
    // only for deserialization:
    protected ClusterStatusMessage() { }

    public ClusterStatusMessage(long creationTime, long lastUpdated,
            NodeState local, Collection<NodeState> localPeers,
            Collection<NodeState> remotePeers)
    {
        this.creationTime = creationTime;
        this.clusterLastUpdated = lastUpdated;
        this.local = local;

        if (localPeers == null) {
            this.localPeers = Collections.emptyList();
        } else {
            this.localPeers = localPeers;
        }

        if (remotePeers == null) {
            this.remotePeers = Collections.emptyList();
        } else {
            this.remotePeers = remotePeers;
        }
    }
}
