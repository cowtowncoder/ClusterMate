package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.clustermate.api.ClusterStatusMessage;

public interface NodeStatusUpdater
{
    public void updateStatus(ClusterStatusMessage msg);
}
