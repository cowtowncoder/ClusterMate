package com.fasterxml.clustermate.service.remote;

import com.fasterxml.clustermate.api.NodeState;

public class RemoteNodeState extends NodeState
{
    public RemoteNodeState(RemoteClusterNode src, NodeState localNode)
    {
        this.address = src._address;
        this.index = -1; // 
//        this.lastUpdated = src.
        this.rangeActive = src.getActiveRange();
        this.rangePassive = src.getPassiveRange();
        this.rangeSync = localNode.totalRange().intersection(src.getTotalRange());
    }
}
