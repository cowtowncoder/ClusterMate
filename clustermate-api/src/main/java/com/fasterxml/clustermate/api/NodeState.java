package com.fasterxml.clustermate.api;

import com.fasterxml.storemate.shared.IpAndPort;

public class NodeState
{
    /**
     * End point for node in question
     */
    protected IpAndPort address;

    /**
     * Index of the node in the ring; either derived from the
     * cluster configuration, or assigned externally to the node.
     */
    protected int index;
    
    /**
     * Timestamp of last update by node itself to this
     * state information; propagated by other nodes, used
     * for determining most recent update. Always time from
     * node itself.
     */
    protected long lastUpdated;
    protected KeyRange rangeActive;
    protected KeyRange rangePassive;
    protected KeyRange rangeSync;
    protected boolean disabled;
    protected long lastSyncAttempt;

    /**
     * Timestamp of earliest possible new entry to discover: that is, all
     * entries prior to this timestamp have been synchronized for covered
     * range.
     */
    protected long syncedUpTo;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    /**
     * Default constructor only used by deserializer; must nominally set
     * fields to default values even thought deserializer will set
     * actual values afterwards.
     */
    protected NodeState()
    {
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
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */
    
    /**
     * Public HTTP entry point (host, port) for the node.
     */
    public IpAndPort getAddress() {
        return address;
    }

    /**
     * Timestamp of last update by node itself to this
     * state information; propagated by other nodes, used
     * for determining most recent update. Always time from
     * node itself.
     */
    public long getLastUpdated() {
        return lastUpdated;
    }
    
    public KeyRange getRangeActive() {
        return rangeActive;
    }

    public KeyRange getRangePassive() {
        return rangePassive;
    }

    public KeyRange getRangeSync() {
        return rangeSync;
    }
    
    public boolean isDisabled() {
        return disabled;
    }

    public long getLastSyncAttempt() {
        return lastSyncAttempt;
    }

    public long getSyncedUpTo() {
        return syncedUpTo;
    }

    /*
    /**********************************************************************
    /* Mutators (for deserialization only; ok to be protected)
    /**********************************************************************
     */

    /*
    public void setAddress(IpAndPort v) {
        address = v;
    }

    public void setLastUpdated(long v) {
        lastUpdated = v;
    }
    
    public void setRangeActive(KeyRange v) {
        rangeActive = v;
    }

    public void setRangePassive(KeyRange v) {
        rangePassive = v;
    }

    public void setRangeSync(KeyRange v) {
        rangeSync = v;
    }
    
    public void setDisabled(boolean v) {
        disabled = v;
    }

    public void setLastSyncAttempt(long v) {
        lastSyncAttempt = v;
    }

    public void setSyncedUpTo(long v) {
        syncedUpTo = v;
    }
    */
    
    /*
    /**********************************************************************
    /* Additional accessors
    /**********************************************************************
     */

    public KeyRange totalRange() {
        KeyRange active = getRangeActive();
        KeyRange passive = getRangePassive();
        if (active == null) {
            return passive;
        }
        if (passive == null) {
            return active;
        }
        return active.union(passive);
    }
}
