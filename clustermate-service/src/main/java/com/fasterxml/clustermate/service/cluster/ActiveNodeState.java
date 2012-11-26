package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.NodeDefinition;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * POJO used for exchanging node state information between servers;
 * also stored in the local database.
 */
public final class ActiveNodeState extends NodeState
{
    /**
     * Lazily calculated value
     */
    protected int _hashCode;

    /*
    /**********************************************************************
    /* Instance creation, conversions
    /**********************************************************************
     */

    // just for (JSON) deserialization
    @SuppressWarnings("unused")
    private ActiveNodeState() {
        super();
        this.address = null;
        this.index = 0;
        this.lastUpdated = 0L;
        this.rangeActive = null;
        this.rangePassive = null;
        this.rangeSync = null;
        this.disabled = false;
        this.lastSyncAttempt = 0L;
        this.syncedUpTo = 0L;
    }
    
    /**
     * Constructor used for constructing initial entry, when no persisted
     * information is available. As such, will not contain valid sync range
     * information, but just range information for node itself.
     */
    public ActiveNodeState(NodeDefinition localNode, long creationTime)
    {
        address = localNode.getAddress();
        index = localNode.getIndex();
        rangeActive = localNode.getActiveRange();
        rangePassive = localNode.getPassiveRange();
        // can't calculate sync range without more info:
        rangeSync = rangeActive.withLength(0);
        disabled = false; // assume nodes start as active
        lastUpdated = 0L;
        lastSyncAttempt = creationTime;
        syncedUpTo = 0L;
    }
    
    /**
     * Constructor used for initializing state from definitions; used
     * first time configurations are read.
     * 
     * @param localNode definition of local node (one on which service runs)
     * @param remoteNode definition of a peer node; node for which state
     *    is to be created (and which will be synced with local node)
     * @param updateTime Timestamp when last update was made; usually passed
     *    as 0L
     */
    public ActiveNodeState(NodeState localNode,
            NodeDefinition remoteNode, long updateTime)
    {
        address = remoteNode.getAddress();
        index = remoteNode.getIndex();
        rangeActive = remoteNode.getActiveRange();
        rangePassive = remoteNode.getPassiveRange();
        lastUpdated = updateTime;

        // need to know total range of remove node
        KeyRange remoteRange = rangeActive.union(rangePassive);
        KeyRange localRange = localNode.totalRange();

        // any overlap?
        if (remoteRange.overlapsWith(localRange)) { // yes!
            rangeSync = remoteRange.union(localRange);
        } else { // nope; create empty sync range
            rangeSync = remoteRange.withLength(0);
        }
        disabled = false; // assume nodes start as active
        lastSyncAttempt = 0L;
        syncedUpTo = 0L;
    }

    /**
     * Constructor called when creating a peer node from information returned
     * piggy-backed on Sync List response.
     */
    public ActiveNodeState(NodeState localNode,
            NodeState remoteNode, long updateTime)
    {
        if (localNode.getAddress().equals(remoteNode.getAddress())) { // sanity check
            throw new IllegalArgumentException("Trying to create Peer with both end points as: "+localNode.getAddress());
        }
        
        address = remoteNode.getAddress();
        index = remoteNode.getIndex();
        rangeActive = remoteNode.getRangeActive();
        rangePassive = remoteNode.getRangePassive();
        lastUpdated = updateTime;

        // need to know total range of remove node
        KeyRange remoteRange = rangeActive.union(rangePassive);
        KeyRange localRange = localNode.totalRange();

        // any overlap?
        if (remoteRange.overlapsWith(localRange)) { // yes!
            rangeSync = remoteRange.union(localRange);
        } else { // nope; create empty sync range
            rangeSync = remoteRange.withLength(0);
        }
        disabled = remoteNode.isDisabled();
        lastSyncAttempt = 0L;
        syncedUpTo = 0L;
    }
    
    // used via fluent factory
    private ActiveNodeState(ActiveNodeState src,
            KeyRange newSyncRange, long newSyncedUpTo)
    {
        address = src.address;
        index = src.index;
        lastUpdated = src.lastUpdated;
        rangeActive = src.rangeActive;
        rangePassive = src.rangePassive;
        rangeSync = newSyncRange;
        disabled = src.disabled;
        lastSyncAttempt = src.lastSyncAttempt;
        syncedUpTo = newSyncedUpTo;
    }

    // used via fluent factory
    private ActiveNodeState(ActiveNodeState src,
            long lastUpdated, long lastSyncAttempt, long syncedUpTo)
    {
        address = src.address;
        index = src.index;
        this.lastUpdated = lastUpdated;
        rangeActive = src.rangeActive;
        rangePassive = src.rangePassive;
        rangeSync = src.rangeSync;
        disabled = src.disabled;
        this.lastSyncAttempt = lastSyncAttempt;
        this.syncedUpTo = syncedUpTo;
    }

    // used via fluent factory
    private ActiveNodeState(ActiveNodeState src, int newIndex)
    {
        address = src.address;
        index = newIndex;
        lastUpdated = src.lastUpdated;
        rangeActive = src.rangeActive;
        rangePassive = src.rangePassive;
        rangeSync = src.rangeSync;
        disabled = src.disabled;
        lastSyncAttempt = src.lastSyncAttempt;
        syncedUpTo = src.syncedUpTo;
    }
    
    /*
    /**********************************************************************
    /* Fluent factories
    /**********************************************************************
     */

    public ActiveNodeState withSyncRange(KeyRange newRange, long newSyncedUpTo) {
        return new ActiveNodeState(this, newRange, newSyncedUpTo);
    }

    /**
     * Fluent factory for creating new state with sync range calculated
     * using specified local node state.
     */
    public ActiveNodeState withSyncRange(ActiveNodeState localNode) {
        long syncTimestamp = syncedUpTo;
        KeyRange newSync = localNode.totalRange().intersection(totalRange());
        if (!rangeSync.contains(newSync)) {
            syncTimestamp = 0L;
        }
        return new ActiveNodeState(this, newSync, syncTimestamp);
    }

    public ActiveNodeState withLastUpdated(long timestamp) {
    	// sanity check: MUST NOT use older timestamp
    	if (timestamp <= lastUpdated) {
    	    if (timestamp == this.lastUpdated) {
    	        return this;
    	    }
    	    throw new IllegalArgumentException("Trying to set earlier 'lastUpdated': had "
    	            +this.lastUpdated+"; trying to set as "+timestamp);
    	}
        return new ActiveNodeState(this, timestamp, lastSyncAttempt, syncedUpTo);
    }

    public ActiveNodeState withLastSyncAttempt(long timestamp) {
        if (lastSyncAttempt == timestamp) {
            return this;
        }
        return new ActiveNodeState(this, lastUpdated, timestamp, syncedUpTo);
    }

    public ActiveNodeState withSyncedUpTo(long timestamp) {
        if (syncedUpTo == timestamp) {
            return this;
        }
        return new ActiveNodeState(this, lastUpdated, lastSyncAttempt, timestamp);
    }

    public ActiveNodeState withIndex(int newIndex) {
        if (index == newIndex) {
            return this;
        }
        return new ActiveNodeState(this, index);
    }
    
    /*
    /**********************************************************************
    /* NodeState methods
    /**********************************************************************
     */

    @Override
    public IpAndPort getAddress() { return address; }

    /**
     * Timestamp of last update by node itself to this
     * state information; propagated by other nodes, used
     * for determining most recent update. Always time from
     * node itself.
     */
    @Override
    public long getLastUpdated() { return lastUpdated; }
    
    @Override
    public KeyRange getRangeActive() { return rangeActive; }

    @Override
    public KeyRange getRangePassive() { return rangePassive; }

    @Override
    public KeyRange getRangeSync() { return rangeSync; }
    
    @Override
    public boolean isDisabled() { return disabled; }

    @Override
    public long getLastSyncAttempt() { return lastSyncAttempt; }

    @Override
    public long getSyncedUpTo() { return syncedUpTo; }

    /*
    /**********************************************************************
    /* Standard methods
    /**********************************************************************
     */

    @Override
    public String toString() {
        return String.valueOf(address);
    }

    /**
     * Equality is specifically defined to encompass just a subset of information:
     *<ul>
     * <li>Endpoint (ip+port)
     *  </li>
     * <li>Disabled?
     *  </li>
     * <li>Active Range
     *  </li>
     * <li>Passive Range
     *  </li>
     * <li>Index
     *  </li>
     *</ul>
     */
    @Override
    public boolean equals(Object o)
    {
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != getClass()) return false;
        ActiveNodeState other = (ActiveNodeState) o;
        if (hashCode() != other.hashCode()) { // should be cheap(er)
            return false;
        }
        // but not enough: must verify they are equal...
        if (index != other.index
                || disabled != other.disabled) {
            return false;
        }
        if (!address.equals(other.address)) {
            return false;
        }
        if (!_rangesEqual(rangeActive, other.rangeActive)
                || !_rangesEqual(rangePassive, other.rangePassive)) {
            return false;
        }
        return true;
    }

    private final static boolean _rangesEqual(KeyRange r1, KeyRange r2) {
        if (r1 == null) {
            return (r2 == null);
        }
        if (r2 == null) {
            return false;
        }
        return r1.equals(r2);
    }
    
    @Override
    public int hashCode()
    {
        int h = _hashCode;
        if (h == 0) {
            h = disabled ? 1 : -1;
            h += index;
            h ^= address.hashCode();
            if (rangeActive != null) {
                h += rangeActive.hashCode();
            }
            if (rangePassive != null) {
                h ^= rangePassive.hashCode();
            }
            _hashCode = h;
        }
        return h;
    }
}
