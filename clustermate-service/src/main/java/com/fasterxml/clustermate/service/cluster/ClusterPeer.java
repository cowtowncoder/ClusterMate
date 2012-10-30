package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Object that contains active state about a single peer for the local node.
 * There is one instance of this class for each store node in cluster,
 * except for the local node itself.
 */
public abstract class ClusterPeer 
//    implements VManaged
{
    /*
    /**********************************************************************
    /* State access
    /**********************************************************************
     */

    public abstract int getFailCount();
    public abstract void resetFailCount();

    public abstract long getSyncedUpTo();

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */

    public abstract IpAndPort getAddress();
    public abstract KeyRange getActiveRange();
    public abstract KeyRange getTotalRange();

    /**
     * Accessor for getting key range that is shared between the local node
     * and this peer; for non-overlapping nodes this may be an empty range.
     */
    public abstract KeyRange getSyncRange();
}
