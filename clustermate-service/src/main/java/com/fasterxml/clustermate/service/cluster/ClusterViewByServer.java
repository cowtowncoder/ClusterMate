package com.fasterxml.clustermate.service.cluster;

import java.util.*;

import com.fasterxml.clustermate.api.ClusterStatusMessage;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.VManaged;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Class that defines information that service components need
 * to access, wrt state of cluster as seen from perspective of
 * a single service node.
 */
public abstract class ClusterViewByServer
    implements VManaged
{
    /*
    /**********************************************************************
    /* Simple accessors
    /**********************************************************************
     */

    public abstract int size();

    public abstract KeySpace getKeySpace();
    
    public abstract NodeState getLocalState();

    public abstract NodeState getRemoteState(IpAndPort key);

    // need generic type to avoid casts when accessing impl
    public abstract List<ClusterPeer> getPeers();

    public abstract Collection<NodeState> getRemoteStates();
    
    public abstract long getLastUpdated();

    /*
    /**********************************************************************
    /* Advanced accessors
    /**********************************************************************
     */

    public abstract int getActiveCoverage();

    public abstract int getActiveCoveragePct();

    public abstract int getTotalCoverage();
    
    public abstract int getTotalCoveragePct();

    public abstract ClusterStatusMessage asMessage();
    
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
     * Method for calculating hash code over shared date, used for determining
     * whether state as observed by this node has changed materially.
     */
    public abstract long getHashOverState();

    /**
     * Method called to let cluster check whether given node is known;
     * and if not, start boostrapping process. This is typically called
     * as a side effect of another operation, and only contains bare
     * minimal to get things started.
     */
    public abstract void checkMembership(IpAndPort node, KeyRange totalRange);
    
    /*
    /**********************************************************************
    /* VManaged methods to implement
    /**********************************************************************
     */

    @Override
    public abstract void start();

    @Override
    public abstract void stop();
}
