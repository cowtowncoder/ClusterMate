package com.fasterxml.clustermate.service.remote;

import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Value class used to keep track of state
 * of a single cluster node.
 * Instances are mutable to a degree, and properly synchronized to allow
 * thread-safe use.
 */
public class RemoteClusterNode
{
    /**
     * Address (ip number and port) used for communicating with the
     * node. Note that this is resolved end point in case server
     * node has been configured to use "localhost" (resolution can
     * be done since client has remote address for starting points)
     */
    protected final IpAndPort _address;

    protected KeyRange _activeRange;
    protected KeyRange _passiveRange;
    protected KeyRange _totalRange;

    /**
     * Time when last request was sent specifically for this server node
     * (i.e. not updated when we get indirect updates)
     */
    protected final AtomicLong _lastRequestSent = new AtomicLong(0L);

    /**
     * Time when last request was sent specifically from this server node
     * (i.e. not updated when we get indirect updates)
     */
    protected final AtomicLong _lastResponseReceived = new AtomicLong(0L);

    /**
     * Timestamp of last update for information regarding this node; regardless
     * of whether directly or indirectly.
     */
    protected long _lastNodeUpdateFetched = 0L;

    /**
     * Timestamp of last version of cluster update from this server node
     * (i.e. not applicable for indirect updates)
     */
    protected long _lastClusterUpdateFetched = 0L;

    /**
     * Timestamp of last version of cluster update that this server node
     * might have; received indirectly via one of GET, PUT or DELETE
     * operations.
     */
    protected final AtomicLong _lastClusterUpdateAvailable = new AtomicLong(1L);

    /*
    /**********************************************************************
    /* Instance creation
    /**********************************************************************
     */

    public RemoteClusterNode(IpAndPort address, KeyRange activeRange, KeyRange passiveRange)
    {
        _address = address;
        _activeRange = activeRange;
        _passiveRange = passiveRange;
        _totalRange = _activeRange.union(_passiveRange);
    }

    public NodeState asNodeState(NodeState localNode) {
        return new RemoteNodeState(this, localNode);
    }
    
    /*
    /**********************************************************************
    /* Mutations
    /**********************************************************************
     */

    public boolean updateRanges(KeyRange activeRange, KeyRange passiveRange)
    {
        if (_activeRange.equals(activeRange) && _passiveRange.equals(passiveRange)) {
            return false;
        }
        _activeRange = activeRange;
        _passiveRange = passiveRange;
        _totalRange = activeRange.union(passiveRange);
        return true;
    }

    public void setLastRequestSent(long timestamp) {
        _lastRequestSent.set(timestamp);
    }

    public void setLastResponseReceived(long timestamp) {
        _lastResponseReceived.set(timestamp);
    }

    public void setLastNodeUpdateFetched(long timestamp) {
        _lastNodeUpdateFetched = timestamp;
    }

    public void setLastClusterUpdateFetched(long timestamp) {
        _lastClusterUpdateFetched = timestamp;
    }

    public void setLastClusterUpdateAvailable(long timestamp) {
        _lastClusterUpdateAvailable.set(timestamp);
    }

    /*
    /**********************************************************************
    /* ReadOnlyServerNodeState implementation (public accessors)
    /**********************************************************************
     */

    public IpAndPort getAddress() { return _address; }

    public KeyRange getActiveRange() { return _activeRange; }

    public KeyRange getPassiveRange() { return _passiveRange; }
    public KeyRange getTotalRange() { return _totalRange; }

    public long getLastRequestSent() { return _lastRequestSent.get(); }
    public long getLastResponseReceived() { return _lastResponseReceived.get(); }

    public long getLastNodeUpdateFetched() { return _lastNodeUpdateFetched; }
    public long getLastClusterUpdateFetched() { return _lastClusterUpdateFetched; }

    public long getLastClusterUpdateAvailable() { return _lastClusterUpdateAvailable.get(); }

    @Override
    public String toString() {
        return "["+_address+"/"+_totalRange+"]";
    }
}
