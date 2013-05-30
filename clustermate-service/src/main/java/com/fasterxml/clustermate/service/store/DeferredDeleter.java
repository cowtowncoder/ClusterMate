package com.fasterxml.clustermate.service.store;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.clustermate.service.store.DeferredDeletionQueue.Action;
import com.fasterxml.clustermate.service.util.SimpleLogThrottler;
import com.fasterxml.storemate.shared.StorableKey;
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
    
    protected final DeferredDeletionQueue<DeferredDeletion> _deletes;

    protected final StorableStore _entryStore;
    
    protected final Thread _deleteThread;

    private final AtomicBoolean _active = new AtomicBoolean(true);
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public DeferredDeleter(DeferredDeletionQueue<DeferredDeletion> deletes, StorableStore entryStore)
    {
        _deletes = deletes;
        _entryStore = entryStore;
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
    
    @Override
    public void start() throws Exception {
        // we are good, nothing much to do...
    }

    @Override
    public void prepareForStop() throws Exception
    {
        // not sure what to do now; should we start failing DELETE additions?
        // Add latency? Just log?
        
        // ... for now we do... nothing. And hope delete queue drains adequately.
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
    
    public Action queueDeletion(long currentTime, StorableKey id) throws InterruptedException
    {
        return _deletes.queueOperation(new DeferredDeletion(currentTime, id));
    }
    
    /*
    /**********************************************************************
    /* Main processing loop
    /**********************************************************************
     */

    protected void processQueue()
    {
        while (_active.get()) {
            DeferredDeletion entry;
            try {
                entry = _deletes.unqueueOperationBlock();
            } catch (InterruptedException e) {
                continue; // most likely we are done
            }
            try {
                _entryStore.softDelete(entry.key, true, true);
            } catch (Throwable t) {
                _throttledLogger.logError("Failed to process deferred delete for entry with key {}: {}", entry.key, t);
            }
        }
        int left = _deletes.size();
        if (left > 0) {
            LOG.warn("Deferred-deletes queue NOT empty when ending", left);
        }
    }
}
