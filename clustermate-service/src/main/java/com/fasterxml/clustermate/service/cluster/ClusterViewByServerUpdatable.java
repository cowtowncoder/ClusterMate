package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.clustermate.api.ClusterStatusMessage;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.VManaged;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Intermediate class that exposes additional callback methods
 * for components that need to feed back cluster status updates.
 */
public abstract class ClusterViewByServerUpdatable
    extends ClusterViewByServer
    implements VManaged
{
    /*
    /**********************************************************************
    /* Methods for cluster membership handling
    /**********************************************************************
     */
    
    /**
     * Method called to add information about cluster state, as piggy-backed
     * on responses other than explicit cluster state requests.
     * 
     * @param response Response to modify
     * 
     * @return Original response object; only returned to allow call chaining, instance
     *   never different from passed-in argument.
     */
    public abstract ServiceResponse addClusterStateInfo(ServiceResponse response);

    /**
     * Method called to add information about cluster state caller has when making
     * Sync List request.
     */
    public abstract RequestPathBuilder addClusterStateInfo(RequestPathBuilder requestBuilder);
    
    /**
     * Method called to let cluster check whether given node is known;
     * and if not, start boostrapping process. This is typically called
     * as a side effect of another operation, and only contains bare
     * minimal to get things started.
     */
    public abstract void checkMembership(IpAndPort node, KeyRange totalRange);

    /**
     * Method called to update cluster view information based on a message
     * returned by a peer; should choose more up-to-date information if
     * any available.
     */
    public abstract void updateWith(ClusterStatusMessage msg);
}
