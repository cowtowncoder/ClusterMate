package com.fasterxml.clustermate.service.remote;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.storemate.shared.IpAndPort;

public class RemoteClusterHandler
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    /**
     * And let's wait for 30 seconds after failed contact attempt to remote cluster
     */
    private final static long MSECS_TO_WAIT_AFTER_FAILED_STATUS = 30 * 1000L;
    
    /**
     * And let's wait up to 10 seconds for remote cluster state messages.
     */
    private final static int MAX_WAIT_SECS_FOR_REMOTE_STATUS = 10;

//    private final static long SLEEP_FOR_SYNCLIST_ERRORS_MSECS = 10000L;

    private final static long SLEEP_FOR_SYNCPULL_ERRORS_MSECS = 3000L;

    /**
     * We'll do bit of sleep before starting remote-sync in general;
     * 10 seconds should be enough.
     */
    private final static long SLEEP_INITIAL = 10 * 1000L;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Configuration
    ///////////////////////////////////////////////////////////////////////
     */

    protected final SharedServiceStuff _stuff;

    protected final RemoteClusterStateFetcher _remoteFetcher;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // State
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Flag used to request termination of the sync thread.
     */
    protected final AtomicBoolean _running = new AtomicBoolean(false);

    protected Thread _syncThread;

    /**
     * State of the remote cluster as we see it, with respect to local node
     * (and remote peers relevant to it).
     */
    protected final AtomicReference<RemoteCluster> _remoteCluster = new AtomicReference<RemoteCluster>();
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, init
    ///////////////////////////////////////////////////////////////////////
     */

    public RemoteClusterHandler(SharedServiceStuff stuff,
            Set<IpAndPort> bs, NodeState localNode)
    {
        _stuff = stuff;
        _remoteFetcher = new RemoteClusterStateFetcher(stuff, _running, bs, localNode);
    }

    public RemoteCluster getRemoteCluster() {
        return _remoteCluster.get();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // StartAndStoppable
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public void start() throws Exception {
        Thread t;
        
        synchronized (this) {
            t = _syncThread;
            if (t == null) { // sanity check
                _running.set(true);
                _syncThread = t = new Thread(new Runnable() {
                    @Override
                    public void run() {

                        // First things first; lazy initialization, do initial DNS lookups to catch probs
                        if (!_remoteFetcher.init()) { // no valid IPs?
                            LOG.error("No valid end points found for {}: CAN NOT PROCEED WITH REMOTE SYNC",
                                    getName());
                        } else {
                            // and then looping if valid endpoints
                            syncLoop();
                        }
                    }
                });
                _syncThread.setDaemon(true);
                _syncThread.setName(getName());
                t.start();
            }
        }
    }

    @Override
    public void prepareForStop() throws Exception {
        _stop(false);
    }

    @Override
    public void stop() throws Exception {
        _stop(true);
    }

    protected void _stop(boolean forced)
    {
        // stopSyncing():
        Thread t;
        synchronized (this) {
            _running.set(false);
            t = _syncThread;
            if (t != null) {
                _syncThread = null;
                LOG.info("Stop requested (force? {}) for {} thread", forced, getName());
            }
        }
        if (t != null) {
//            t.notify();
            t.interrupt();
        }
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Update loop
    ///////////////////////////////////////////////////////////////////////
     */

    protected void syncLoop()
    {
//        public RemoteCluster fetch(int maxWaitSecs) throws IOException
        
        LOG.info("Starting {} thread", getName());

        // TODO: any special handling for tests? Or just avoid running altogether?
        /*
        if (_stuff.isRunningTests()) {
            try {
                _timeMaster.sleep(1L);
            } catch (InterruptedException e) { }
        }
         */
        
        // Delay start just slightly since startup is slow time, don't want to pile
        // lots of background processing
        try {
            Thread.sleep(SLEEP_INITIAL);
        } catch (InterruptedException e) { }

        /* At high level, we have two kinds of tasks, depending on whether
         * there is any overlap:
         * 
         * 1. If ranges overlap, we need to do proper sync list/pull handling
         * 2. If no overlap, we just need to keep an eye towards changes, to
         *   try to keep whole cluster view up to date (since clients need it)
         */
        while (_running.get()) {

            try {
                RemoteCluster remote = _remoteCluster();

                if (remote == null) {
                    continue;
                }

                LOG.info("Got Remote Cluster info: "+remote);
                
                // !!! TBI
                Thread.sleep(8000L);
                
                /*
                if (hasOverlap(_cluster.getLocalState(), _syncState)) {
                    doRealSync();
                } else {
                    doMinimalSync();
                }
                */
                /*
            } catch (InterruptedException e) {
                if (_running.get()) {
                    LOG.warn("syncLoop() interrupted without clearing '_running' flag; ignoring");
                }
                */
            } catch (Exception e) {
                LOG.warn("Uncaught processing exception during syncLoop(): ({}) {}",
                        e.getClass().getName(), e.getMessage());
                if (_running.get()) {
                    // Ignore failures during shutdown, so only increase here
                    try {
                        _stuff.getTimeMaster().sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
                    } catch (InterruptedException e2) { }
                }
            }
        }
    }

    protected RemoteCluster _remoteCluster() throws IOException
    {
        // We still have valid setup?
        RemoteCluster rc = _remoteCluster.get();
        if (rc == null || !rc.isStillValid(_stuff.currentTimeMillis())) {
            rc = _remoteFetcher.fetch(MAX_WAIT_SECS_FOR_REMOTE_STATUS);
            if (_remoteCluster == null) {
                final long timeoutMsecs = MSECS_TO_WAIT_AFTER_FAILED_STATUS;
                LOG.warn("Failed to access remote cluster status information; will wait for {} msecs",
                        timeoutMsecs);
                try {
                    _stuff.getTimeMaster().sleep(timeoutMsecs);
                } catch (InterruptedException e) { }
            }
            _remoteCluster.set(rc);
        }
        return rc;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected String getName() {
        return "RemoteClusterSync";
    }
}
