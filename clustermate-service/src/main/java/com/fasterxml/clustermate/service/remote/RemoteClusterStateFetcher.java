package com.fasterxml.clustermate.service.remote;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;

import com.fasterxml.clustermate.api.ClusterStatusAccessor;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.clustermate.json.ClusterMessageConverter;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.std.JdkClusterStatusAccessor;

/**
 * Helper class used for accessing status of remote cluster, for purpose
 * of syncing from one of peers of this instance.
 *<p>
 * NOTE: lots of duplication with StoreClientBootstrapper, unfortunately.
 */
public class RemoteClusterStateFetcher
{
    /**
     * Not sure where this should be located; probably not here.
     * But since the other definition is in <code>VagabondClientConfigBuilder</code>,
     * which is inaccessible from  here, this'll have to do for now.
     */
    protected final static String[] DEFAULT_BASE_PATH = new String[] { "apicursorfile", "v" };

    /**
     * Let's keep initial timeouts relatively low, since we can usually
     * try to go through multiple server nodes to get response quickly.
     */
    public final static long BOOTSTRAP_TIMEOUT_MSECS = 2000L;

    /**
     * Let's limit validity of Remote cluster view info to 15 minutes, so that
     * information is reloaded every now and then.
     */
    private final static long MSECS_FOR_REMOTE_RELOAD = 15 * 60 * 1000L;

    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Configuration
    ///////////////////////////////////////////////////////////////////////
     */

    protected final SharedServiceStuff _config;

    /**
     * Set of server nodes used for bootstrapping; we need at least
     * one to be able to locate others.
     */
    protected final Set<IpAndPort> _initialEndpoints;

    /**
     * Object used to access Node State information, needed to construct
     * view of the cluster.
     */
    protected ClusterStatusAccessor _accessor;

    /**
     * We need bit of local state for negotiating with remote peer.
     */
    protected final NodeState _localNode;

    protected final AtomicBoolean _running;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */
	
    public RemoteClusterStateFetcher(SharedServiceStuff stuff,
            AtomicBoolean running,
            Set<IpAndPort> endpoints, NodeState local)
    {
        _config = stuff;
        _running = running;
        _initialEndpoints = endpoints;
        _localNode = local;
        _accessor = new JdkClusterStatusAccessor(new ClusterMessageConverter(stuff.jsonMapper()),
                DEFAULT_BASE_PATH, stuff.getPathStrategy());
    }

    /**
     * @return Whether at least one of endpoints was resolvable or not
     */
    protected boolean init()
    {
        LOG.info("Try to resolve {} remote IP endpoints", _initialEndpoints.size());
        Iterator<IpAndPort> it = _initialEndpoints.iterator();
        while (it.hasNext()) {
            if (!_running.get()) {
                LOG.warn("Terminating remote IP endpoint resolution");
                return false;
            }
            IpAndPort ip = it.next();
            try {
                ip.getIP();
            } catch (Exception e) {
                LOG.error("Failed to resolve end point '"+ip.toString()+"', removing. Problem: "+e.getMessage());
                it.remove();
            }
        }
        LOG.info("Completed remote IP endpoint resolution");
        return true;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Client bootstrapping
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method called to fetch state information from the remote cluster so that
     * it is possible to determine optimal peer matching, and start making
     * sync-list/pull calls.
     * 
     * @return Fully initialized remote cluster info object, if successful; or
     *    null to indicate that fetch failed
     */
    public RemoteCluster fetch(int maxWaitSecs) throws IOException
    {
        final long startTime = _config.currentTimeMillis();
        final long waitUntil = (maxWaitSecs <= 0) ? Long.MAX_VALUE : (startTime + 1000 * maxWaitSecs);

        // Since we'll be removing entries, need to make a local copy:
        ArrayList<IpAndPort> ips = new ArrayList<IpAndPort>(_initialEndpoints);
        BootstrapState bs = new BootstrapState();

        long roundStart;
        int round = 0;

        while (!ips.isEmpty() && (roundStart = System.currentTimeMillis()) < waitUntil) {
            Iterator<IpAndPort> it = ips.iterator();
            while (it.hasNext()) {
                IpAndPort ip = it.next();
                final long requestTime = System.currentTimeMillis();
                long maxTimeout = waitUntil - requestTime;
                try {
                    ClusterStatusMessage resp = _accessor.getRemoteStatus(ip,
                            Math.min(maxTimeout, BOOTSTRAP_TIMEOUT_MSECS));
                    if (resp == null) {
                        continue;
                    }
                    it.remove(); // remove from bootstrap list
                    NodeState peer = resp.local;
                    bs.updateDirectState(ip, peer,
                            requestTime, System.currentTimeMillis(), resp.creationTime);
                    // Important: include LOCAL peers of REMOTE that we called; must NOT
                    // Cross the Lines
                    for (NodeState stateSec : resp.localPeers) {
                        bs.updateIndirectState(ip, stateSec);
                    }
                    // and we don't really care about remote peers here
                } catch (IOException e) {
                    // only warn during first round
                    if (round == 0) {
                        LOG.warn("Failed to access Remote Status on {} ({}: {}), will retry",
                                ip, e.getClass().getName(), e.getMessage());
                    }
                } catch (RuntimeException e) { // usually more severe ones, NPE etc
                    LOG.error("Internal error with cluster state call (IP "+ip+"): "
                            +"("+e.getClass().getName()+") "+e.getMessage(), e);
                } catch (Exception e) {
                    LOG.error("Initial cluster state call (IP "+ip+") failed: ("+e.getClass().getName()+") "
                            +e.getMessage(), e);
                }
            }
            ++round;

            // If we failed first time around, let's wait a bit...
            long timeTaken = System.currentTimeMillis() - roundStart;
            if (timeTaken < 1000L) { // if we had string of failures, wait a bit
                try {
                    Thread.sleep(1000L - timeTaken);
                } catch (InterruptedException e) {
                    throw new IOException(e.getMessage(), e);
                }
            }
        }
        return bs.finish(_config.getTimeMaster(), _localNode);
    }

/**
     * Helper class used for aggregating state of remote cluster during boostrapping.
     */
    static class BootstrapState
    {
        protected final Map<IpAndPort, RemoteClusterNode> _nodes = new HashMap<IpAndPort, RemoteClusterNode>();

        /*
        ///////////////////////////////////////////////////////////////////////
        // Life-cycle
        ///////////////////////////////////////////////////////////////////////
         */
        
        public BootstrapState() { }

        public RemoteCluster finish(TimeMaster timeMaster, NodeState localState)
        {
            List<RemoteClusterNode> overlapping = new ArrayList<RemoteClusterNode>();
            final KeyRange localRange = localState.totalRange();
            for (RemoteClusterNode node : _nodes.values()) {
                if (localRange.overlapsWith(node.getTotalRange())) {
                    overlapping.add(node);
                }
            }
            if (overlapping.isEmpty()) {
                return null;
            }
            // Also, need to sort in descending order of preference
            Collections.sort(overlapping, new Comparator<RemoteClusterNode>() {
                @Override
                public int compare(RemoteClusterNode c1, RemoteClusterNode c2) {
                    KeyRange r1 = c1.getTotalRange();
                    // First, exact match is the usual way, preferred
                    if (r1.equals(localRange)) {
                        return -1;
                    }
                    KeyRange r2 = c2.getTotalRange();
                    if (r2.equals(localRange)) {
                        return 1;
                    }

                    // If not, let's actually base it on stable clockwise-distance between
                    // starting points, such that we'll try to find range that starts as soon
                    // as possible _after_ local range
                    int dist1 = localRange.clockwiseDistance(r1);
                    int dist2 = localRange.clockwiseDistance(r2);

                    if (dist1 != dist2) {
                        return dist1 - dist2;
                    }
                    
                    // And if this still doesn't resolve, choose one with _smaller_ range; assumption
                    // is that most likely cluster is growing, and smaller range indicates newer
                    // information
                    int diff = r1.getLength() - r2.getLength();
                    if (diff != 0) { // sort from smaller to bigger
                        return diff;
                    }
                    
                    // and otherwise use simple lexicographic (~= alphabetic) ordering of endpoint IP
                    // Goal being a stable ordering
                    
                    return c1.getAddress().compareTo(c2.getAddress());
                }
            });
            return new RemoteCluster(timeMaster.currentTimeMillis() + MSECS_FOR_REMOTE_RELOAD, localState,
                    overlapping);
        }

        /*
        ///////////////////////////////////////////////////////////////////////
        // State updates
        ///////////////////////////////////////////////////////////////////////
         */
        
        /**
         * Method called to add information directly related to node that served
         * the request.
         */
        public void updateDirectState(IpAndPort byNode, NodeState stateInfo,
                long requestTime, long responseTime,
                long clusterInfoVersion)
        {
            RemoteClusterNode localState = _nodes.get(byNode);
            if (localState == null) { // new info 
                localState = new RemoteClusterNode(byNode, stateInfo.getRangeActive(), stateInfo.getRangePassive());
                _addNode(byNode, localState);
            }
            /*boolean needInvalidate =*/ localState.updateRanges(stateInfo.getRangeActive(),
                    stateInfo.getRangePassive());
            localState.setLastRequestSent(requestTime);
            localState.setLastResponseReceived(responseTime);
            localState.setLastNodeUpdateFetched(stateInfo.getLastUpdated());
        }

        /**
         * Method called to add information obtained indirectly; i.e. "gossip".
         */
        public void updateIndirectState(IpAndPort byNode, NodeState stateInfo)
        {
            // First: ensure references are properly resolved (eliminate "localhost" if need be):
            IpAndPort ip = stateInfo.getAddress();
            if (ip.isLocalReference()) {
                ip = byNode.withPort(ip.getPort());
            }
            final long nodeInfoTimestamp = stateInfo.getLastUpdated();
            // otherwise pretty simple:
            RemoteClusterNode state = _nodes.get(ip);
            if (state == null) { // new info 
                state = new RemoteClusterNode(ip, stateInfo.getRangeActive(), stateInfo.getRangePassive());
                _addNode(ip, state);
            } else {
                // quick check to ensure info we get is newer: if not, skip
                if (nodeInfoTimestamp <= state.getLastNodeUpdateFetched()) {
                    return;
                }
            }
            state.setLastNodeUpdateFetched(nodeInfoTimestamp);
            /*boolean needInvalidate =*/ state.updateRanges(stateInfo.getRangeActive(),
                    stateInfo.getRangePassive());
        }

        private void _addNode(IpAndPort key, RemoteClusterNode state)
        {
            _nodes.put(key, state);
            // and, if had other things to add, they'd get initialized here as well
        }
    }
}
