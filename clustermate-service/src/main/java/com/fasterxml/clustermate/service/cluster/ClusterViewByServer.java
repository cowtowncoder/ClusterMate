package com.fasterxml.clustermate.service.cluster;

import java.util.*;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Class that defines information that service components need
 * to access, wrt state of cluster as seen from perspective of
 * a single service node.
 */
public abstract class ClusterViewByServer
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

    /**
     * Helper method that can be called to see whether given key may be
     * handled by the local service, according to its current definitions.
     */
    public abstract boolean containsLocally(EntryKey k);
    
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

    /**
     * Method for calculating hash code over shared date, used for determining
     * whether state as observed by this node has changed materially.
     */
    public abstract long getHashOverState();

    /*
    /**********************************************************************
    /* Other misc functionality for adding cluster info on messages
    /**********************************************************************
     */
    
    /**
     * Method called to add information about cluster state caller has when making
     * Sync List request.
     */
    public abstract RequestPathBuilder addClusterStateInfo(RequestPathBuilder requestBuilder);

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

}
