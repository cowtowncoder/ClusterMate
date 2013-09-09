package com.fasterxml.clustermate.service.cleanup;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.TimeMaster;

/**
 * Helper class that handles details of background cleanup
 * processing, related to data storage and expiration.
 */
public class CleanerUpper<K extends EntryKey, E extends StoredEntry<K>>
    implements Runnable, StartAndStoppable
{
    private final Logger LOG;

    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */

    protected final SharedServiceStuff _stuff;
    protected final TimeSpan _delayBetweenCleanups;
    
    /*
    /**********************************************************************
    /* Related helper objects
    /**********************************************************************
     */

    protected final TimeMaster _timeMaster;

    protected final Stores<K,E> _stores;
    
    /**
     * Object that keeps track of observed cluster state.
     */
    protected ClusterViewByServer _cluster;

    /*
    /**********************************************************************
    /* Other state
    /**********************************************************************
     */

    /**
     * Flag used for communicating need to shut down processing.
     */
    protected final AtomicBoolean _shutdown = new AtomicBoolean(false);

    /**
     * State flag used to indicate clean shutdown.
     */
    protected final AtomicBoolean _completed = new AtomicBoolean(false);

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
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public CleanerUpper(SharedServiceStuff stuff, Stores<K,E> stores,
            ClusterViewByServer cluster,
            List<CleanupTask<?>> tasks)
    {
        this(stuff, stores, cluster, tasks, null);
    }

    public CleanerUpper(SharedServiceStuff stuff, Stores<K,E> stores,
            ClusterViewByServer cluster,
            List<CleanupTask<?>> tasks,
            Logger logger)
    {
        if (logger == null) {
            logger = LoggerFactory.getLogger(getClass());
        }
        LOG = logger;
        _stuff = stuff;
        _timeMaster = stuff.getTimeMaster();
        _stores = stores;
        _cluster = cluster;
        _delayBetweenCleanups = stuff.getServiceConfig().cfgDelayBetweenCleanup;
        // Important: start with LocalEntryCleaner (to try to avoid dangling files),
        // then do FileCleaner
        _tasks = tasks.toArray(new CleanupTask[tasks.size()]);
        for (CleanupTask<?> task : _tasks) {
            task.init(_stuff, _stores, _cluster, _shutdown);
        }
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
    public void prepareForStop() {
        // Let's mark the fact we are shutting down, but not yet terminate
        // on-going processes.
        // NOTE! Tasks are also given this marker so that they also know when
        // to bail out
        _shutdown.set(true);
        CleanupTask<?> curr = _currentTask.get();
        if (curr != null) {
            curr.prepareForStop();
        }
    }

    @Override
    public void stop()
    {
        if (!_completed.get() && (_thread != null)) {
            CleanupTask<?> curr = _currentTask.get();
            // with actual task running, need to be bit more careful?
            if (curr != null) {
                LOG.warn("CleanerUpper not complete when stop() called: will wait a bit first");
                boolean complete = false;

                // up to 1 second only
                try {
                    for (int i = 0; i < 20; ++i) {
                        Thread.sleep(50L);
                        complete = _completed.get();
                    }
                } catch (InterruptedException e) { }

                if (!complete) {
                    LOG.warn("CleanerUpper task '{}' not complete when stop() called: will try Thread.interrupt()",
                            curr.toString());
                    _thread.interrupt();
                }
            } else { // otherwise... should be fine
                LOG.warn("CleanerUpper not complete when stop() called, but no task running: will try Thread.interrupt()");
                _thread.interrupt();
            }
        }
    }

    /*
    /**********************************************************************
    /* Main loop
    /**********************************************************************
     */

    @Override
    public void run()
    {
        try {
            while (!_shutdown.get()) {
                _runOnce();
            }
        } finally {
            _completed.set(true);
        }
    }
    
    protected void _runOnce()
    {
        long delayMsecs = _nextStartTime.get() - _timeMaster.currentTimeMillis();
        if (delayMsecs > 0L) {
            LOG.info("Waiting up to {} seconds until running cleanup tasks...",
                    (delayMsecs / 1000L));
            try {
                _timeMaster.sleep(delayMsecs);
            } catch (InterruptedException e) {
                final long msecs = _nextStartTime.get() - _timeMaster.currentTimeMillis();
                LOG.info("Interruped during wait for next runtime ({} before next run)", TimeMaster.timeDesc(msecs));
                return;
            }
        }
        final long startTime = _timeMaster.currentTimeMillis();
        // ok, run...
        LOG.info("Starting cleanup tasks ({})", _tasks.length);
        _nextStartTime.set(startTime + _delayBetweenCleanups.getMillis());
        for (CleanupTask<?> task : _tasks) {
            if (_shutdown.get()) {
                if (!_stuff.isRunningTests()) {
                    LOG.info("Stopping cleanup tasks due to shutdown");
                }
                break;
            }
            _currentTask.set(task);
            try {
                Object result = task.cleanUp();
                long took = _timeMaster.currentTimeMillis() - startTime;
                LOG.info("Clean up task {} complete in {}, result: {}",
                        task.getClass().getName(), TimeMaster.timeDesc(took), result);
            } catch (Exception e) {
                LOG.warn("Problems running clean up task of type "+task.getClass().getName()+": "+e.getMessage(), e);
            } finally {
                _currentTask.set(null);
            }
        }
        long tookAll = _timeMaster.currentTimeMillis() - startTime;
        LOG.info("Completing clean up tasks in {}", TimeMaster.timeDesc(tookAll));
    }

    /*
    /**********************************************************************
    /* Other methods
    /**********************************************************************
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
        return "Waiting for "+TimeMaster.timeDesc(msecs)+" until next cleanup round";
    }


    /*
    /**********************************************************************
    /* Overridable logging
    /**********************************************************************
     */
}
