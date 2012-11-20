package com.fasterxml.clustermate.api;

import java.util.Collection;
import java.util.Collections;

/**
 * POJO used as response for GET on cluster status.
 */
public class ClusterStatusMessage
{
    /**
     * Status of the local node.
     */
    public NodeState local;

    public Collection<NodeState> remote;

    /**
     * Timestamp of last update to aggregated cluster information by the
     * serving server node. Used for synchronization such that client
     * can get indirect updates on what is the latest available time;
     * and the responses can then be matched. This can be used to both
     * speed up lookups and reduce unnecessary cluster status lookup calls.
     */
    public long clusterLastUpdated;
    
    // only for deserialization:
    protected ClusterStatusMessage() { }

    public ClusterStatusMessage(long lastUpdated,
            NodeState local, Collection<NodeState> remote)
    {
        clusterLastUpdated = lastUpdated;
        this.local = local;
        if (remote == null) {
            this.remote = Collections.emptyList();
        } else {
            this.remote = remote;
        }
    }
}
