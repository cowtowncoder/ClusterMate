package com.fasterxml.clustermate.service.cluster;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ClusterStatusAccessor;
import com.fasterxml.clustermate.api.ClusterStatusMessage;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.json.ClusterMessageConverter;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.bdb.NodeStateStore;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.std.JdkClusterStatusAccessor;

public class ClusterViewByServerImpl<K extends EntryKey, E extends StoredEntry<K>>
    extends ClusterViewByServerUpdatable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    /**
     * Key space used by this cluster.
     */
    protected final KeySpace _keyspace;
    
    /**
     * Persistent data store in which we store information regarding
     * synchronization.
     */
    protected final NodeStateStore _stateStore;
    
    /**
     * Information about this node; not included in the list of peer nodes.
     */
    protected NodeState _localState;

    /**
     * States of all nodes found during bootstrapping, including the
     * local node.
     */
    protected final Map<IpAndPort, ClusterPeerImpl<K,E>> _peers;

    protected final TimeMaster _timeMaster;
    
    /**
     * Timestamp of the last update to aggregated state; used for letting
     * clients know whether to try to access updated cluster information
     */
    protected final AtomicLong _lastUpdated;

    protected final boolean _isTesting;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public ClusterViewByServerImpl(SharedServiceStuff stuff, Stores<K,E> stores,
            KeySpace keyspace,
            ActiveNodeState local, Map<IpAndPort,ActiveNodeState> remoteNodes,
            long updateTime)
    {
        _localState = local;
        _keyspace = keyspace;
        _stateStore = stores.getNodeStore();
        _timeMaster = stuff.getTimeMaster();
        _isTesting = stuff.isRunningTests();
        ServiceConfig config = stuff.getServiceConfig();
        ClusterStatusAccessor accessor = new JdkClusterStatusAccessor(new ClusterMessageConverter(
                stuff.jsonMapper()),
                config.servicePathRoot, config.getServicePathStrategy());

        _peers = new LinkedHashMap<IpAndPort,ClusterPeerImpl<K,E>>(remoteNodes.size());
        for (Map.Entry<IpAndPort,ActiveNodeState> entry : remoteNodes.entrySet()) {
            _peers.put(entry.getKey(), new ClusterPeerImpl<K,E>(stuff, this,
                    stores.getNodeStore(), stores.getEntryStore(), entry.getValue(),
                    accessor));
        }
        _lastUpdated = new AtomicLong(updateTime);
    }

    @Override
    public synchronized void start()
    {
        LOG.info("Starting sync threads to peers...");
        int count = 0;
        // no need to sync yet
        for (final ClusterPeerImpl<?,?> peer : _peers.values()) {
            /* 20-Nov-2012, tatu: Let's create a thread for every peer from now
             *   on; even if there is currently no sync range, things may change.
             *   Plus, for fast cluster view updates, may want to share info
             *   between non-neighbors too.
             */
            ++count;
            peer.startSyncing();
        }
        LOG.info("Completed creation of sync threads ({}/{}) to peers", count, _peers.size());
    }

    @Override
    public synchronized void stop()
    {
        LOG.info("Shutting down sync threads to peers...");
        
        for (ClusterPeerImpl<?,?> peer : _peers.values()) {
            peer.stop();
        }
        LOG.info("Completed shutting down sync threads to peers");
    }

    /*
    /**********************************************************************
    /* Simple accessors
    /**********************************************************************
     */
    
    /**
     * Returns size of cluster, which should be one greater than number of
     * peer nodes (to add local node)
     */
    @Override
    public int size() { return 1 + _peers.size(); }

    @Override
    public KeySpace getKeySpace() { return _keyspace; }
    
    @Override
    public NodeState getLocalState() { return _localState; }

    @Override
    public NodeState getRemoteState(IpAndPort key) {
        ClusterPeerImpl<?,?> peer;
        synchronized (_peers) {
            peer = _peers.get(key);
        }
        return (peer == null) ? null : peer.getSyncState();
    }

    @Override
    public List<ClusterPeer> getPeers() {
        synchronized (_peers) {
            return new ArrayList<ClusterPeer>(_peers.values());
        }
    }

    @Override
    public Collection<NodeState> getRemoteStates() {
        ArrayList<NodeState> result = new ArrayList<NodeState>(_peers.size());
        for (ClusterPeerImpl<?,?> peer : _peers.values()) {
            result.add(peer.getSyncState());
        }
        return result;
    }
    
    @Override
    public long getLastUpdated() {
        return _lastUpdated.get();
    }

    /*
    /**********************************************************************
    /* NodeStatusUpdater impl
    /**********************************************************************
     */

    /**
     * Method called with a response to node status request; will update
     * information we have if more recent information is available.
     */
    @Override
    public void updateWith(ClusterStatusMessage msg)
    {
        final long updateTime = _timeMaster.currentTimeMillis();
        int mods = 0;
        NodeState local = msg.local;
        if (local == null) { // should never occur
            LOG.info("msg.local is null, should never happen");
        } else {
            if (updateStatus(local)) {
                ++mods;
            }
        }
        if (msg.remote != null) {
            for (NodeState state : msg.remote) {
                if (updateStatus(state)) {
                    ++mods;
                }
            }
        }

        // If any data was changed, update our local state
        if (mods > 0) {
            boolean wasChanged;
            synchronized (_lastUpdated) {
                long old = _lastUpdated.get();
                wasChanged = (updateTime > old);
                if (wasChanged) {
                    _lastUpdated.set(updateTime);
                }
            }
            if (wasChanged) {
                LOG.info("updateStatus() with {} changes: updated lastUpdated to: {}", mods, updateTime);
            } else { // may occur with concurrent updates?
                LOG.warn("updateStatus() with {} changes: but lastUpdated remains at {}", mods,
                        _lastUpdated.get());
            }
        }
    }

    protected synchronized boolean updateStatus(NodeState nodeStatus)
    {
        LOG.info("Should try to update node status for: {}", nodeStatus.getAddress());
        // !!! TODO
        return false;
    }
    
    /*
    /**********************************************************************
    /* Advanced accessors
    /**********************************************************************
     */

    @Override
    public int getActiveCoverage()
    {
        ArrayList<KeyRange> ranges = new ArrayList<KeyRange>();
        ranges.add(_localState.getRangeActive());
        for (ClusterPeer peer : getPeers()) {
            ranges.add(peer.getActiveRange());
        }
        return _keyspace.getCoverage(ranges);
    }

    @Override
    public int getActiveCoveragePct() {
        return _coveragePct(getActiveCoverage());
    }

    @Override
    public int getTotalCoverage()
    {
        ArrayList<KeyRange> ranges = new ArrayList<KeyRange>();
        ranges.add(_localState.totalRange());
        for (ClusterPeer peer : getPeers()) {
            ranges.add(peer.getTotalRange());
        }
        return _keyspace.getCoverage(ranges);
    }
    
    @Override
    public int getTotalCoveragePct() {
        return _coveragePct(getTotalCoverage());
    }

    @Override
    public ClusterStatusMessage asMessage() {
        return new ClusterStatusMessage(_timeMaster.currentTimeMillis(),
                getLastUpdated(), getLocalState(), getRemoteStates());
    }

    @Override
    public long getHashOverState()
    {
        int hash = _keyspace.hashCode();
        hash ^= _localState.hashCode();
        for (ClusterPeerImpl<?,?> peer : _peerImpls()) {
            hash += peer.getSyncState().hashCode();
        }
        return hash;
    }
    
    /*
    /**********************************************************************
    /* Cluster membership handling
    /**********************************************************************
     */

    @Override
    public ServiceResponse addClusterStateInfo(ServiceResponse response)
    {
        long clusterUpdated = getLastUpdated();
        return response.addHeader(ClusterMateConstants.CUSTOM_HTTP_HEADER_LAST_CLUSTER_UPDATE,
                clusterUpdated);
    }

    @Override
    public RequestPathBuilder addClusterStateInfo(RequestPathBuilder requestBuilder)
    {
        /* Since key range information will be included anyway, all we need here
         * is just the endpoint name ("caller").
         */
        requestBuilder = requestBuilder.addParameter(ClusterMateConstants.HTTP_QUERY_PARAM_CALLER,
                _localState.getAddress().toString());
        return requestBuilder;
    }

    @Override
    public void checkMembership(IpAndPort node, KeyRange totalRange)
    {
        // First, a sanity check:
        if (_localState.getAddress().equals(node)) {
            LOG.warn("checkMembership() called with local node address; ignoring");
            return;
        }

        // Then actual business...
        boolean alreadySeen;
        synchronized (_peers) {
            alreadySeen = _peers.containsKey(node);
        }

        // !!!! TODO
        if (alreadySeen) {
            LOG.info("Already know about end point: "+node+", ignoring");
        } else {
            LOG.warn("Did NOT know about end point: "+node+": SOMEONE SHOULD DO SOMETHING!");
        }
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    private int _coveragePct(int absCoverage) {
        int len = _keyspace.getLength();
        if (absCoverage == len) {
            return 100;
        }
        return (int) ((100.0 * absCoverage) / len);
    }

    protected List<ClusterPeerImpl<K,E>> _peerImpls() {
        synchronized (_peers) {
            return new ArrayList<ClusterPeerImpl<K,E>>(_peers.values());
        }
    }
}
