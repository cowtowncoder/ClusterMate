package com.fasterxml.clustermate.service.cluster;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.IpAndPort;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.bdb.NodeStateStore;
import com.fasterxml.clustermate.service.store.StoredEntry;

public class ClusterViewByServerImpl<K extends EntryKey, E extends StoredEntry<K>>
    extends ClusterViewByServer
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

    /**
     * Timestamp of the last update to aggregated state; used for letting
     * clients know whether to try to access updated cluster information
     */
    protected long _lastUpdated;

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
        
        _peers = new LinkedHashMap<IpAndPort,ClusterPeerImpl<K,E>>(remoteNodes.size());
        for (Map.Entry<IpAndPort,ActiveNodeState> entry : remoteNodes.entrySet()) {
            _peers.put(entry.getKey(), new ClusterPeerImpl<K,E>(stuff,
                    stores.getNodeStore(), stores.getEntryStore(), entry.getValue()));
        }
        _lastUpdated = updateTime;
    }

    @Override
    public synchronized void start()
    {
        LOG.info("Starting sync threads to peers...");
        int count = 0;
        for (ClusterPeerImpl<?,?> peer : _peers.values()) {
            if (peer.getSyncRange().empty()) {
                LOG.info("No shared key range to {}, skipping", peer.getAddress());
                continue;
            }
            LOG.info("Shared key range of {} to {}", peer.getSyncRange(), peer.getAddress());
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
        ClusterPeerImpl<?,?> peer = _peers.get(key);
        return (peer == null) ? null : peer.getSyncState();
    }

    @Override
    public List<ClusterPeer> getPeers() {
     return new ArrayList<ClusterPeer>(_peers.values());
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
        return _lastUpdated;
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

    /*
    /**********************************************************************
    /* Integration with front-end (does it belong here?)
    /**********************************************************************
     */

    @Override
    public ServiceResponse addClusterStateHeaders(ServiceResponse response)
    {
        long clusterUpdated = getLastUpdated();
        return response.addHeader(ClusterMateConstants.CUSTOM_HTTP_HEADER_LAST_CLUSTER_UPDATE,
                clusterUpdated);
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
}
