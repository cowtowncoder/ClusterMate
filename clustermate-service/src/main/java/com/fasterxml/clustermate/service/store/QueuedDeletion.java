package com.fasterxml.clustermate.service.store;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Container for queued instance of delete operation.
 */
class QueuedDeletion
{
    protected final StorableKey _key;

    protected final long _expirationTime;

    protected final Thread _callerThread;
    
    protected final AtomicReference<DeletionResult> _statusRef;
    
    public QueuedDeletion(StorableKey key, long expirationTime,
            Thread callerThread)
    {
        _key = key;
        _expirationTime = expirationTime;
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
