package com.fasterxml.clustermate.service.store;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.clustermate.service.cfg.DeferredDeleteConfig;
import com.fasterxml.clustermate.service.metrics.DeferQueueMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalOperationMetrics;
import com.fasterxml.clustermate.service.util.DecayingAverageCalculator;
import com.fasterxml.clustermate.service.util.SimpleLogThrottler;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;

/**
 * Helper class used for handling deletions asynchronously.
 */
public class DeferredDeleter
    implements StartAndStoppable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    /**
     * We may get shit storms of failures, so let's throttle output for possibly
     * voluminous errors to 2 per second...
     */
    protected final SimpleLogThrottler _throttledLogger = new SimpleLogThrottler(LOG, 500);

    protected final StorableStore _entryStore;

    protected final ArrayBlockingQueue<QueuedDeletion> _deletions;

    protected final DecayingAverageCalculator _averages;
    
    protected final TimeMaster _timeMaster;

    protected final int _minDeferQLength;
    protected final int _maxDeferQLength;

    protected final int _targetMaxQueueDelayMicros;

    protected final int _maxQueueDelayMsecs;
    
    protected final Thread _deleteThread;

    /**
     * We will try to estimate maximum queue length to allow, based
     * on maximum delay target and average 
     */
    protected final AtomicInteger _currentMaxQueueLength;
    
    private final AtomicBoolean _active = new AtomicBoolean(true);
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public DeferredDeleter(StorableStore entryStore,
            DeferredDeleteConfig config)
    {
        _timeMaster = entryStore.getTimeMaster();
        
        _minDeferQLength = config.minQueueLength;
        _maxDeferQLength = config.maxQueueLength;

        _targetMaxQueueDelayMicros = 1000 * Math.max(1, (int) config.queueTargetDelayMsecs.getMillis());
        // Start with minimum length...
        _currentMaxQueueLength = new AtomicInteger(_minDeferQLength);  
        _maxQueueDelayMsecs = (int) config.queueMaxDelayMsecs.getMillis();
        
        /* We need at least 'maxDefQLength' entries for deferred (unblocking)
         * entries; but also up to N extras for blocking. Since we do not
         * know for sure N, let's use conservative upper bound of 1000; it's
         * much higher than any thread count allocated for deletions.
         */
        _deletions = new ArrayBlockingQueue<QueuedDeletion>(Math.max(0, _maxDeferQLength) + 1000);
        _entryStore = entryStore;
        
        /* We will also try to estimate how long it would take to complete
         * given delete operation as deferred deletion; we will ONLY take
         * deferrals up to certain maximum delay, after which blocking
         * will be needed to give feedback to caller.
         * The main idea here is to optimize for normal steady state, during
         * which all deletes should ideally be deferred.
         * 
         * Parameters: average over past 100 samples; start with assumption of
         * 10 msec per sample (should be way lower -- note: units are in 1024s of
         * msecs!); limit variation to factor of 5.0x
         */
        _averages = new DecayingAverageCalculator(100, 10 * 1024, 5.0);
        _deleteThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    processQueue();
                } finally {
                    LOG.info("Deferred-deleter queue update thread ended.");
                }
            }
        });
        _deleteThread.setName("DeferredDeleter");
        _deleteThread.setDaemon(true);
        _deleteThread.start();
    }
 
    public static DeferredDeleter nonDeferring(StorableStore entryStore)
    {
        DeferredDeleteConfig config = new DeferredDeleteConfig();
        config.minQueueLength = 0;
        config.maxQueueLength = 0;
        return new DeferredDeleter(entryStore, config);
    }
    
    @Override
    public void start() throws Exception {
        // we are good, nothing much to do...
    }

    @Override
    public void prepareForStop() throws Exception
    {
        // not sure what to do now; should we start failing DELETE additions?
        // Add latency? Just log?
        
        // ... for now we do... nothing. And hope delete queue drains adequately?
    }

    @Override
    public void stop() throws Exception
    {
        _active.set(false);
        _deleteThread.interrupt();
    }

    /*
    /**********************************************************************
    /* Simple API for feeding us
    /**********************************************************************
     */

    public DeletionResult addDeferredDeletion(StorableKey key, long currentTime)
    {
        if (_canDefer(currentTime)) {
            // no expiration, no Thread to unpark:
            final QueuedDeletion del = new QueuedDeletion(key, 0L, null);
            if (!_deletions.offer(del)) {
                // should never occur but:
                return DeletionResult.forQueueFull();
            }
            return DeletionResult.forDeferred();
        }
        return addNonDeferredDeletion(key, currentTime);
    }

    public DeletionResult addNonDeferredDeletion(StorableKey key, long currentTime)
    {
        Thread currThread = Thread.currentThread();
        final QueuedDeletion del = new QueuedDeletion(key,
                currentTime+_maxQueueDelayMsecs, currThread);
        
        if (!_deletions.offer(del)) { // should never occur either...
            return DeletionResult.forQueueFull();
        }
        DeletionResult status;
        do {
            LockSupport.park();
            status = del.getStatus();
        } while (status == null);
        return status;
    }

    protected boolean _canDefer(long currentTime)
    {
        if (_maxDeferQLength <= 0) {
            return false;
        }
        return (_deletions.size() < _currentMaxQueueLength.get());
    }

    /*
    /**********************************************************************
    /* Method(s) to expose metrics
    /**********************************************************************
     */
    
    protected void augmentMetrics(ExternalOperationMetrics deleteMetrics)
    {
        DeferQueueMetrics q = new DeferQueueMetrics();
        q.minLength = _minDeferQLength;
        q.maxLength = _maxDeferQLength;
        q.currentLength = _deletions.size();
        q.maxLengthForDefer = _currentMaxQueueLength.get();
        q.delayTargetMsecs = _targetMaxQueueDelayMicros / 1000;
        // and then get estimated average per-operation delay (note: is in usecs)
        q.estimatedDelayMsecs = (_averages.getCurrentAverage() / 1000.0);
        deleteMetrics.queue = q;
    }

    /*
    /**********************************************************************
    /* Main processing loop
    /**********************************************************************
     */

    /* Batch operations are more efficient than individual ones, and this
     * even extends to this seemingly trivial case -- based on measurements,
     * doing this does speed things up (probably since sync'ed access to
     * blokcing queue may trigger context switch?)
     */
    private final static int CHUNK_SIZE = 10;
    
    protected void processQueue()
    {
        final ArrayList<QueuedDeletion> buffer = new ArrayList<QueuedDeletion>(CHUNK_SIZE);
        
        while (_active.get()) {
            // Start by bit of draining action, to catch up with backlog
            int count;
            try {
                count = _deletions.drainTo(buffer, CHUNK_SIZE);
                if (count == 0) { // but if none found, revert to blocking...
                    QueuedDeletion del = _deletions.take();
                    final long nanoStart = System.nanoTime();
                    if (_delete(del, _timeMaster.currentTimeMillis())) {
                        long micros = (System.nanoTime() - nanoStart) >> 10;
                        int newAvg = _averages.addSample((int) micros);
                        _updateMaxQueue(newAvg);
                    }
                    del.wakeUpCaller();
                    continue;
                }
            } catch (InterruptedException e) { // most likely means we are done...
                continue;
            }
            final long nanoStart = System.nanoTime();
            final long systemTime = _timeMaster.currentTimeMillis();
            int okCount = 0;
            for (int i = 0; i < count; ++i) {
                // only consider actual deletions to count for time estimation purposes
                if (_delete(buffer.get(i), systemTime)) {
                    ++okCount;
                }
            }
            if (okCount > 0) {
                long micros = ((System.nanoTime() - nanoStart) / okCount) >> 10;
                
                // if we get full chunk, add more weight
                int newAvg;
                if (count == CHUNK_SIZE) {
                    newAvg = _averages.addRepeatedSample((int) micros, 2);
                } else {
                    newAvg = _averages.addSample((int) micros);
                }
                _updateMaxQueue(newAvg);
            }
            for (int i = 0; i < count; ++i) {
                buffer.get(i).wakeUpCaller();
            }
            buffer.clear();
        }
        int left = _deletions.size();
        if (left > 0) {
            LOG.warn("Deferred-deletes queue NOT empty when ending", left);
        }
    }

    // protected to give access to unit tests (ditto for return value)
    private int _updateMaxQueue(int newAvgMicros)
    {
        // first things first: add bit of time for overhead (say, 1/16 == 6.25%)
        newAvgMicros += (newAvgMicros >> 4);
        // and then calculate max length, given 

        int len = _targetMaxQueueDelayMicros / newAvgMicros;
        if (len > _maxDeferQLength) {
            len = _maxDeferQLength;
        } else if (len < _minDeferQLength) {
            len = _minDeferQLength;
        }
        
//LOG.info("DELETE-defer-length using {} msec estimate -> {}", newAvgMicros/1000.0, len);
        
        _currentMaxQueueLength.set(len);
        return len;
    }

    /**
     * @return True if deletion succeeded; used to only include valid timings
     *   for estimation
     */
    private final boolean _delete(QueuedDeletion deletion, long currentTime)
    {
        // First things first: are we timed out already?
        if (deletion.isExpired(currentTime)) {
            deletion.setStatus(DeletionResult.forTimeOut());
            return false;
        }
        
        try {
            _entryStore.softDelete(deletion.getKey(), true, true);
            deletion.setStatus(DeletionResult.forCompleted());
        } catch (Throwable t) {
            deletion.setFail(t);
            return false;
        }
        return true;
    }
}
