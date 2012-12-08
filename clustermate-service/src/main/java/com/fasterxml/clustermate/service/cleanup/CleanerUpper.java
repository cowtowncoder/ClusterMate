package com.fasterxml.clustermate.service.cleanup;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.TimeMaster;

/**
 * Helper class that handles details of background cleanup
 * processing, related to data storage and expiration.
 */
public class CleanerUpper<K extends EntryKey, E extends StoredEntry<K>>
    implements Runnable, StartAndStoppable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /*
    ///////////////////////////////////////////////////////////////////////
    // Configuration
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected final TimeSpan _delayBetweenCleanups;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Related helper objects
    ///////////////////////////////////////////////////////////////////////
     */

    protected final TimeMaster _timeMaster;
    
    /**
     * Object that keeps track of observed cluster state.
     */
    protected ClusterViewByServer _cluster;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Other state
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Flag used for communicating need to shut down processing.
     */
    protected final AtomicBoolean _shutdown = new AtomicBoolean(false);

    /**
     * Actual clean up thread we use
     */
    protected Thread _thread;
    
    /**
     * Timestamp of next time that cleanup tasks get triggered.
     */
    protected AtomicLong _nextStartTime = new AtomicLong(0L);
    
    /**
     * Reference to currently active cleanup component; used for
     * producing status descriptions.
     */
    protected AtomicReference<CleanupTask<?>> _currentTask = new AtomicReference<CleanupTask<?>>();

    protected final CleanupTask<?>[] _tasks;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Life-cycle
    ///////////////////////////////////////////////////////////////////////
     */
    
    public CleanerUpper(SharedServiceStuff stuff, Stores<K,E> stores,
            ClusterViewByServer cluster)
    {
        _cluster = cluster;
        _timeMaster = stuff.getTimeMaster();
        _delayBetweenCleanups = stuff.getServiceConfig().cfgDelayBetweenCleanup;
        _tasks = new CleanupTask[] {
                new FileCleaner(stuff, _shutdown),
                new LocalEntryCleaner<K,E>(stuff, stores, _shutdown)
        };
    }

    @Override
    public void start()
    {
        // let's wait 50% of minimum delay; typically 30 minutes
//        long delayMsecs = (_delayBetweenCleanups.toMilliseconds() / 2);

        long delayMsecs = 5000L;
        
        _nextStartTime.set(_timeMaster.currentTimeMillis() + delayMsecs);
        _thread = new Thread(this);
        _thread.start();
    }

    @Override
    public void stop()
    {
        _shutdown.set(true);
        if (_thread != null) {
            _thread.interrupt();
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Main loop
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public void run()
    {
        while (!_shutdown.get()) {
            long delayMsecs = _nextStartTime.get() - _timeMaster.currentTimeMillis();
            if (delayMsecs > 0L) {
                LOG.info("Waiting up to {} seconds until running cleanup tasks...",
                        (delayMsecs / 1000L));
                try {
                    _timeMaster.sleep(delayMsecs);
                } catch (InterruptedException e) {
                    continue;
                }
            }
            final long startTime = _timeMaster.currentTimeMillis();
            // ok, run...
            _nextStartTime.set(startTime + _delayBetweenCleanups.getMillis());
            for (CleanupTask<?> task : _tasks) {
                _currentTask.set(task);
                try {
                    Object result = task.cleanUp();
                    LOG.info("Clean up task {} complete, result: {}",
                            task.getClass().getName(), result);
                } catch (Exception e) {
                    LOG.warn("Problems running clean up task of type "+task.getClass().getName()+": "+e.getMessage(), e);
                }
            }
            _currentTask.set(null);
            long took = _timeMaster.currentTimeMillis() - startTime;
            LOG.info("Completing clean up tasks in {}", _timeMaster.timeDesc(took));
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Other methods
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method is overridden to provide somewhat readable description of
     * current state, to be served externally.
     */
    @Override
    public String toString()
    {
        CleanupTask<?> task = _currentTask.get();
        if (task != null) {
            return "Current task: "+task.toString();
        }
        long msecs = _nextStartTime.get() - _timeMaster.currentTimeMillis();
        if (msecs < 0L) {
            msecs = 0L;
        }
        return "Waiting for "+_timeMaster.timeDesc(msecs)+" until next cleanup round";
    }
}
