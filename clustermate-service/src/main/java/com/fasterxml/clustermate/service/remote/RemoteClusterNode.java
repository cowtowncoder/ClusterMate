package com.fasterxml.clustermate.service.remote;

import com.fasterxml.storemate.shared.IpAndPort;

import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.service.state.ActiveNodeState;

/**
 * Value class used to keep track of state
 * of a single node of remote cluster.
 * Note that unlike with instances used with local cluster nodes,
 * these instances are NOT accessed from multiple threads, and as
 * such no synchronization is used.
 * Above is not to say that all instances are necessarily used from
 * a single thread, but rather that each instance is access from a single
 * thread at a time.
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
    protected long _lastRequestSent = 0L;

    /**
     * Time when last successful response was received
     */
    protected long _lastResponseReceived = 0L;

    /**
     * Timestamp used for figuring out the most recent piece of gossip
     * info for cluster state.
     */
    protected long _lastNodeUpdateFetched;

    protected transient ActiveNodeState _persisted;
    
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

    public ActiveNodeState persisted() { return _persisted; }
    public void setPersisted(ActiveNodeState p) { _persisted = p; }
    
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
        _lastRequestSent = timestamp;
    }

    public void setLastResponseReceived(long timestamp) {
        _lastResponseReceived = timestamp;
    }

    public void setLastNodeUpdateFetched(long timestamp) {
        _lastNodeUpdateFetched = timestamp;
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
    
    public long getLastRequestSent() { return _lastRequestSent; }
    public long getLastResponseReceived() { return _lastResponseReceived; }
    public long getLastNodeUpdateFetched() { return _lastNodeUpdateFetched; }

    @Override
    public String toString() {
        return "["+_address+"/"+_totalRange+"]";
    }
}
