package com.fasterxml.clustermate.client.cluster;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.cluster.ClusterServerNodeImpl;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Class that encapsulates view of the cluster, as whole, from
 * client perspective.
 */
public class ClusterViewByClientImpl<K extends EntryKey>
    extends ClusterViewByClient<K>
{
	/**
	 * Reference to the underlying network client, so that we can
	 * construct paths for requests.
	 */
	private final NetworkClient<K> _client;

	private final KeySpace _keyspace;
    
	private final EntryKeyConverter<K> _keyConverter;
    
	private final Map<IpAndPort, ClusterServerNodeImpl> _nodes = new LinkedHashMap<IpAndPort, ClusterServerNodeImpl>();

    /**
     * Since we will need to iterate over server nodes, let's use pre-calculated
     * array.
     */
    private AtomicReference<ClusterServerNode[]> _states = new AtomicReference<ClusterServerNode[]>(
            new ClusterServerNode[0]);

    private final EntryAccessors<K> _entryAccessors;

    /**
     * Helper object that knows how to find node(s) to send requests to, for
     * given key.
     */
    private final HashRouter<K> _hashRouter;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    /**
     * Path segments for the root path
     */
    private final String[] _rootPathSegments;

    public ClusterViewByClientImpl(StoreClientConfig<K,?> storeConfig,
            NetworkClient<K> client, KeySpace keyspace)
    {
        _keyspace = keyspace;
        if (client == null) {
            _client = null;
            _keyConverter = null;
            _entryAccessors = null;
        } else {
            _client = client;
            _keyConverter = client.getKeyConverter();
            _entryAccessors = client.getEntryAccessors();
        }
        if (storeConfig == null) {
            _rootPathSegments = new String[0];
        } else {
            _rootPathSegments = storeConfig.getBasePath();
        }
        _hashRouter = new HashRouter<K>(keyspace, _keyConverter, _states);
    }
    
    public static <K extends EntryKey> ClusterViewByClientImpl<K> forTesting(KeySpace keyspace)
    {
        return new ClusterViewByClientImpl<K>(null, null, keyspace);
    }

    /*
    protected String[] _splitPath(String base)
    {
        base = base.trim();
        if (base.startsWith("/")) {
            base = base.substring(1);
        }
        if (base.endsWith("/")) {
            base = base.substring(0, base.length()-1);
        }
        if (base.length() == 0) {
            return new String[0];
        }
        return base.split("/");
    }
    */
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

    @Override
    public int getServerCount() {
        return _nodes.size();
    }

    @Override
    public boolean isFullyAvailable() {
        return getCoverage() == _keyspace.getLength();
    }

    @Override
    public int getCoverage() {
        return _getCoverage(_states.get());
    }

    // separate method for testing:
    protected int _getCoverage(ClusterServerNode[] states)
    {
        BitSet slices = new BitSet(_keyspace.getLength());
        for (ClusterServerNode state : states) {
            state.getTotalRange().fill(slices);
        }
        return slices.cardinality();
    }

    @Override
    public NodesForKey getNodesFor(K key) {
        return _hashRouter.getNodesFor(key);
    }

    /*
    /**********************************************************************
    /* Updating state
    /**********************************************************************
     */
    
    /**
     * Method called to add information directly related to node that served
     * the request.
     */
    public synchronized void updateDirectState(IpAndPort byNode, NodeState stateInfo,
            long requestTime, long responseTime,
            long clusterInfoVersion)
    {
        ClusterServerNodeImpl localState = _nodes.get(byNode);
        if (localState == null) { // new info 
            localState = new ClusterServerNodeImpl(_rootPathFor(byNode),
            		byNode, stateInfo.getRangeActive(), stateInfo.getRangePassive(),
                    _entryAccessors);
            _addNode(byNode, localState);
        }
        boolean needInvalidate = localState.updateRanges(stateInfo.getRangeActive(),
                stateInfo.getRangePassive());
        if (localState.updateDisabled(stateInfo.isDisabled())) {
            needInvalidate = true;
        }
        if (needInvalidate) {
            invalidateRouting();
        }
        localState.setLastRequestSent(requestTime);
        localState.setLastResponseReceived(responseTime);
        localState.setLastNodeUpdateFetched(stateInfo.getLastUpdated());
        localState.setLastClusterUpdateFetched(clusterInfoVersion);
    }
    
    /**
     * Method called to add information obtained indirectly; i.e. "gossip".
     */
    public synchronized void updateIndirectState(IpAndPort byNode, NodeState stateInfo)
    {
        // First: ensure references are properly resolved (eliminate "localhost" if need be):
        IpAndPort ip = stateInfo.getAddress();
        if (ip.isLocalReference()) {
            ip = byNode.withPort(ip.getPort());
        }
        final long nodeInfoTimestamp = stateInfo.getLastUpdated();
        // otherwise pretty simple:
        ClusterServerNodeImpl state = _nodes.get(ip);
        if (state == null) { // new info 
            state = new ClusterServerNodeImpl(_rootPathFor(ip),
            		ip, stateInfo.getRangeActive(), stateInfo.getRangePassive(),
                    _entryAccessors);
            _addNode(ip, state);
        } else {
            // quick check to ensure info we get is newer: if not, skip
            if (nodeInfoTimestamp <= state.getLastNodeUpdateFetched()) {
                return;
            }
        }
        state.setLastNodeUpdateFetched(nodeInfoTimestamp);
        boolean needInvalidate = state.updateRanges(stateInfo.getRangeActive(),
                stateInfo.getRangePassive());
        if (state.updateDisabled(stateInfo.isDisabled())) {
            needInvalidate = true;
        }
        if (needInvalidate) {
            invalidateRouting();
        }
    }

    /*
    /**********************************************************************
    /* Test support
    /**********************************************************************
     */

    protected NodesForKey _calculateNodes(int version, KeyHash keyHash,
            ClusterServerNode[] allNodes)
    {
        return _hashRouter._calculateNodes(version, keyHash, allNodes);
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected RequestPath _rootPathFor(IpAndPort serverAddress)
    {
        RequestPathBuilder builder = _client.pathBuilder(serverAddress);
        for (String component : _rootPathSegments) {
            builder = builder.addPathSegment(component);
        }
        return builder.build();
    }
    
    private void _addNode(IpAndPort key, ClusterServerNodeImpl state)
    {
        _nodes.put(key, state);
        _states.set(_nodes.values().toArray(new ClusterServerNodeImpl[_nodes.size()]));
    }

    /**
     * Method called when server node state information changes in a way that
     * may affect routing of requests.
     */
    private final void invalidateRouting() {
        _hashRouter.invalidateRouting();
    }
}
