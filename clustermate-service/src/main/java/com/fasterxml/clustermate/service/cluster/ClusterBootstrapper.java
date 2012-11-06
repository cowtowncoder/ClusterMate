package com.fasterxml.clustermate.service.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.IpAndPort;

import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.api.NodeDefinition;
import com.fasterxml.clustermate.service.ServerUtil;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.bdb.NodeStateStore;
import com.fasterxml.clustermate.service.cfg.NodeConfig;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Helper class which will initialize cluster state and set up.
 */
public class ClusterBootstrapper<K extends EntryKey, E extends StoredEntry<K>>
{
    /* We will delete stale unrecognized entries after 24 hours, to
     * keep Node store somewhat clean.
     */
    private final static long SECS_IN_24H = 24 * 60 * 60;
    
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    protected final SharedServiceStuff _stuff;

    protected final Stores<K,E> _stores;
    
    /**
     * Timestamp we use as "current time": usually system time, but
     * may be hard-coded by tests.
     */
    protected final long _startTime;
    
    protected final KeySpace _keyspace;
    
    public ClusterBootstrapper(long startTime, SharedServiceStuff stuff, Stores<K,E> stores)
    {
        _stuff = stuff;
        _stores = stores;
        _startTime = startTime;
        _keyspace = new KeySpace(stuff.getServiceConfig().cluster.clusterKeyspaceSize);
    }
    
    /**
     * Factory method for constructing state object from static cluster
     * configuration
     * 
     * @param thisInstancePort Port number this instance is listening to; needed to
     *    find correct cluster configuration
     */
    public ClusterViewByServer bootstrap(final int thisInstancePort)
        throws IOException
    {
        // First: need the keyspace definition, to build key ranges
        NodeDefinition localDef = null;

        // Then need to know IP interface(s) host has, to find "local" entry
        Set<InetAddress> localIps = new LinkedHashSet<InetAddress>();
        ServerUtil.findLocalIPs(localIps);
        LOG.info("Local IPs: {}", localIps.toString());
        Map<IpAndPort,NodeDefinition> nodeDefs = new LinkedHashMap<IpAndPort,NodeDefinition>();

        for (NodeDefinition node : _readNodeDefs()) {
            IpAndPort ip = node.getAddress();
            LOG.info("Resolving node definitions for: "+ip.toString());
            // Entry for this instance?
            // NOTE: this call will resolve IP name to address; blocking call:
            InetAddress addr = ip.getIP();
            if (localIps.contains(addr) && ip.getPort() == thisInstancePort) {
                if (localDef != null) {
                    throw new IllegalStateException("Ambiguous definition: both "
                            +localDef.getAddress()+" and "+ip+" refer to this host");
                }
                localDef = node;
                continue;
            }
            LOG.info("peer node: {}", node); // remove once in prod?
            nodeDefs.put(ip, node);
        }
        // Error: MUST have local node definition
        if (localDef == null) {
            throw new IllegalStateException("Could not find Cluster node definitions for local instance (port "
                    +thisInstancePort+")");
        }

        LOG.info("Node definition used for this host: {}, found {} configured peer nodes",
        		localDef, nodeDefs.size());
        
        final NodeStateStore nodes = _stores.getNodeStore();
        // Next: load state definitions from BDB
        List<ActiveNodeState> storedStates = nodes.readAll(_keyspace);
        LOG.info("Read {} persisted node entries from local store", storedStates.size());

        // First things first: find and update node for local node
        ActiveNodeState localAct = _remove(storedStates, localDef.getAddress());
        if (localAct == null) { // need to create?
            if (!_stuff.isRunningTests()) {
                LOG.warn("No persisted entry for local node: will create and store one");
            }
            localAct = new ActiveNodeState(localDef, _startTime);
            // also: this works as the "official" set up time...
            localAct = localAct.withLastUpdated(_startTime);
            nodes.upsertEntry(localAct);
        } else {
            // one more thing: force dummy update on restart as well
            localAct = localAct.withLastUpdated(_startTime);
        }
        
        // then merge entries; create/delete orphans
        Map<IpAndPort,ActiveNodeState> activeState = _mergeStates(nodes,
                localAct, nodeDefs, storedStates);

        LOG.info("Merged state of {} node entries (including local)", activeState.size());

        // We could try to determine stored value; but let's reset on reboot, for now:
        long clusterUpdateTime = _stuff.getTimeMaster().currentTimeMillis();
        return new ClusterViewByServerImpl<K,E>(_stuff, _stores, _keyspace,
                localAct, activeState, clusterUpdateTime);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
    */
    
    protected List<NodeDefinition> _readNodeDefs()
            throws IOException
    {
        ArrayList<NodeDefinition> defs = new ArrayList<NodeDefinition>();
        // And then let's read static definition
        for (NodeConfig node : _stuff.getServiceConfig().cluster.clusterNodes) {
            IpAndPort ip = node.ipAndPort;
            KeyRange range = _keyspace.range(node.keyRangeStart, node.keyRangeLength);
            // we'll use same range for both passive and active ranges, with static
            defs.add(new NodeDefinition(ip, range, range));
        }
        return defs;
    }
    
    /**
     * Helper method that takes both configured node definitions and persisted
     * node states, to create active node definitions and update persisted
     * state if and as necessary.
     */
    protected Map<IpAndPort,ActiveNodeState> _mergeStates(NodeStateStore nodeStore,
    		ActiveNodeState localState, Map<IpAndPort,NodeDefinition> nodeDefs,
            List<ActiveNodeState> storedStates)
    {
        Map<IpAndPort,ActiveNodeState> result = new LinkedHashMap<IpAndPort,ActiveNodeState>();

        // and then we'll go over peer nodes
        Map<IpAndPort,NodeDefinition> orphanDefs = new LinkedHashMap<IpAndPort,NodeDefinition>(nodeDefs);
        LOG.info("Merging {} configured peer entries with {} persistent entries",
                nodeDefs.size(), storedStates.size());
        
        // Iterate over entries that have been persisted:
        for (ActiveNodeState state : storedStates) {
            IpAndPort key = state.address;
            // Then: is there config for that entry? If not, skip or remove:
            NodeDefinition def = orphanDefs.remove(key);
            if (def == null) {
                long staleTimeSecs = (_startTime - state.lastSyncAttempt) / 1000L;
                if (staleTimeSecs < SECS_IN_24H) {
                    LOG.warn("Unrecognized persisted Node state, key {}: less than 24h old, will skip", key);
                    continue;
                }
                // If too old, delete
                if (!_stuff.isRunningTests()) {
                    LOG.warn("Unrecognized persisted Node state, key {}: more than 24h old ({} days), will DELETE",
                            key, (staleTimeSecs / SECS_IN_24H));
                }
                nodeStore.deleteEntry(key);
                continue;
            }
            // We have both config and state: merge
            state = _updatePersistentState(nodeStore, localState, def, state);
           result.put(key, state);
        }

        LOG.info("Any orphan definitions? (node without persisted state) Found {}", orphanDefs.size());
        
        // And then reverse check: any config entries for which we have no state?
        // If we do, need to initialize state
        int i = 0;
        for (NodeDefinition def : orphanDefs.values()) {
            ActiveNodeState state = new ActiveNodeState(def, _startTime);
            state = state.withSyncRange(localState);
            if (!_stuff.isRunningTests()) {
                LOG.warn("Configuration entry without state, key {}: will need to (re)create state (sync range {})",
                        def.getAddress(), state.rangeSync);
            }
            try {
                nodeStore.upsertEntry(state);
            } catch (Exception e) {
                LOG.error("Failed to update node state entry #{}, must skip. Problem ({}): {}",
                        i, e.getClass().getName(), e.getMessage());
            }
            result.put(state.address, state);
            ++i;
        }
        return result;
    }

    private ActiveNodeState _remove(Collection<ActiveNodeState> nodes, IpAndPort key)
    {
        Iterator<ActiveNodeState> it = nodes.iterator();
        while (it.hasNext()) {
            ActiveNodeState curr = it.next();
            if (key.equals(curr.address)) {
                it.remove();
                return curr;
            }
        }
        return null;
    }
    
    /**
     * Helper method that will figure out how to modify persistent state,
     * considering active configuration and persisted state.
     */
    protected ActiveNodeState _updatePersistentState(NodeStateStore nodeStore,
            ActiveNodeState localNode,
            NodeDefinition remoteDef, ActiveNodeState remoteNode)
    {
        // The main thing is to see if sync range has changed...
        final KeyRange oldSyncRange = remoteNode.rangeSync;
        KeyRange newSyncRange = localNode.totalRange().intersection(remoteNode.totalRange());
        
        if (newSyncRange.equals(remoteNode.rangeSync)) {
            LOG.info("Sync range between local and {} unchanged: {}"
                    ,remoteNode.address, newSyncRange);
        } else {
            long syncedTo = remoteNode.syncedUpTo;
            // only need to reset if expanding...
            if (oldSyncRange.contains(newSyncRange)) {
                LOG.info("Sync range between local and {} changed from {} to {}: but new contained in old, no reset"
                        , new Object[] { remoteNode.address, oldSyncRange, newSyncRange});
            } else {
                LOG.warn("Sync range between local and {} changed from {} to {}; new not completely contained in old, MUST reset"
                        , new Object[] { remoteNode.address, oldSyncRange, newSyncRange});
                syncedTo = 0L;
            }
            remoteNode = remoteNode.withSyncRange(newSyncRange, syncedTo);
            try {
                nodeStore.upsertEntry(remoteNode);
            } catch (Exception e) {
                LOG.error("Failed to update node state for {}, must skip. Problem ({}): {}",
                        remoteNode, e.getClass().getName(), e.getMessage());
            }
            // anything else?
        }
        return remoteNode;
    }
}
