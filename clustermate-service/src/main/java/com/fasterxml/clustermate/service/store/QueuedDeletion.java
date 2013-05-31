package com.fasterxml.clustermate.service.store;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Container for queued instance of delete operation. Operation may
 * be "deferred" (async, caller does not wait for completion" or
 * blocked (calling thread blocks to wait for completion).
 */
class QueuedDeletion
{
    /**
     * Raw key of the entry to delete.
     */
    protected final StorableKey _key;

    /**
     * Timestamp of when deletion is to be considered expired, if it has
     * not yet taken place.
     */
    protected final long _expirationTime;

    /**
     * Calling thread for blocking operation; null for deferred deletions.
     */
    protected final Thread _callerThread;

    /**
     * Atomically accessible result object; set by thread that executes
     * (or times out) deletion, read by blocked calling thread.
     */
    protected final AtomicReference<DeletionResult> _statusRef;
    
    public QueuedDeletion(StorableKey key, long expirationTime,
            Thread callerThread)
    {
        _key = key;
        _expirationTime = (expirationTime == 0L) ? Long.MAX_VALUE : expirationTime;
        _callerThread = callerThread;
        _statusRef = new AtomicReference<DeletionResult>(null);
    }

    public boolean wakeUpCaller()
    {
        if (_callerThread == null) {
            return false;
        }
        LockSupport.unpark(_callerThread);
        return true;
    }

    public boolean isExpired(long currentTime) {
        // can't expire without blocked Thread to take care of it...
        return (_callerThread != null)
                && (currentTime > _expirationTime);
    }
    
    public void setFail(Throwable t) {
        _statusRef.set(DeletionResult.forFail(t));
    }
    
    public void setStatus(DeletionResult status) {
        _statusRef.set(status);
    }

    public StorableKey getKey() {
        return _key;
    }
    
    public DeletionResult getStatus() {
        return _statusRef.get();
    }
    
    @Override public String toString() {
        return _key.toString();
    }
}
