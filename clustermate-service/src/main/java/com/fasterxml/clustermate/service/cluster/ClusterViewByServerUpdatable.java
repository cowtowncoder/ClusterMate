package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Intermediate class that exposes additional callback methods
 * for components that need to feed back cluster status updates.
 */
public abstract class ClusterViewByServerUpdatable
    extends ClusterViewByServer
    implements StartAndStoppable
{
    /*
    /**********************************************************************
    /* Methods for cluster membership handling
    /**********************************************************************
     */
    
    /**
     * Method called to let cluster check whether given node is known;
     * and if not, start boostrapping process. This is typically called
     * as a side effect of another operation, and only contains bare
     * minimal to get things started.
     */
    public abstract void checkMembership(IpAndPort node, long timestamp, KeyRange totalRange);

    /**
     * Method called to update cluster view information based on a message
     * returned by a peer; should choose more up-to-date information if
     * any available.
     */
    public abstract void updateWith(ClusterStatusMessage msg);

    /**
     * Method called when we have received an indication that specified node
     * is being activated (is starting up)
     */
    public abstract void nodeActivated(IpAndPort node, long timestamp, KeyRange totalRange);

    /**
     * Method called when we have received an indication that specified node
     * is being deactivated (is shutting down).
     */
    public abstract void nodeDeactivated(IpAndPort node, long timestamp);
}
