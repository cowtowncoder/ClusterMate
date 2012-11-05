package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.NodeDefinition;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * POJO used for exchanging node state information between servers;
 * also stored in the local database.
 */
public class ActiveNodeState extends NodeState
{
    /**
     * End point for node in question
     */
    public final IpAndPort address;

    /**
     * Timestamp of last update by node itself to this
     * state information; propagated by other nodes, used
     * for determining most recent update. Always time from
     * node itself.
     */
    public final long lastUpdated;
    
    public final boolean disabled;

    public final KeyRange rangeActive;

    public final KeyRange rangePassive;

    public final KeyRange rangeSync;

    public final long lastSyncAttempt;

    /**
     * Timestamp of earliest possible new entry to discover: that is, all
     * entries prior to this timestamp have been synchronized for covered
     * range.
     */
    public final long syncedUpTo;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Instance creation, conversions
    ///////////////////////////////////////////////////////////////////////
     */

    // just for (JSON) deserialization
    @SuppressWarnings("unused")
    private ActiveNodeState() {
        this.address = null;
        this.lastUpdated = 0L;
        this.rangeActive = null;
        this.rangePassive = null;
        this.rangeSync = null;
        this.disabled = false;
        this.lastSyncAttempt = 0L;
        this.syncedUpTo = 0L;
    }
    
    /*
    // 01-Nov-2012, tatu: Should actually not be needed -- Reflection allows modifying
    //   immutable fields!
    
    @JsonCreator
    public ActiveNodeState(@JsonProperty("address") IpAndPort address,
            @JsonProperty("lastUpdated") long lastUpdated,
            @JsonProperty("rangeActive") KeyRange rangeActive,
            @JsonProperty("rangePassive") KeyRange rangePassive,
            @JsonProperty("rangeSync") KeyRange rangeSync,
            @JsonProperty("disabled") boolean disabled,
            @JsonProperty("lastSyncAttempt") long lastSyncAttempt,
            @JsonProperty("syncedUpTo") long syncedUpTo
        )
    {
        this.address = address;
        this.lastUpdated = lastUpdated;
        this.rangeActive = rangeActive;
        this.rangePassive = rangePassive;
        this.rangeSync = rangeSync;
        this.disabled = disabled;
        this.lastSyncAttempt = lastSyncAttempt;
        this.syncedUpTo = syncedUpTo;
    }
*/    
    
    /**
     * Constructor used for constructing initial entry, when no persisted
     * information is available. As such, will not contain valid sync range
     * information, but just range information for node itself.
     */
    public ActiveNodeState(NodeDefinition localNode, long creationTime)
    {
        address = localNode.getAddress();
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
    public ActiveNodeState(ActiveNodeState localNode,
            NodeDefinition remoteNode, long updateTime)
    {
        address = remoteNode.getAddress();
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

    // used via fluent factory
    private ActiveNodeState(ActiveNodeState src,
            KeyRange newSyncRange, long newSyncedUpTo)
    {
        address = src.address;
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
        this.lastUpdated = lastUpdated;
        rangeActive = src.rangeActive;
        rangePassive = src.rangePassive;
        rangeSync = src.rangeSync;
        disabled = src.disabled;
        this.lastSyncAttempt = lastSyncAttempt;
        this.syncedUpTo = syncedUpTo;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Fluent factories
    ///////////////////////////////////////////////////////////////////////
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
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // NodeState methods
    ///////////////////////////////////////////////////////////////////////
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
    ///////////////////////////////////////////////////////////////////////
    // Standard methods
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public String toString() {
        return String.valueOf(address);
    }
}
