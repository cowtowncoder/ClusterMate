package com.fasterxml.clustermate.service.cleanup;

import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.storemate.shared.TimeMaster;

public abstract class CleanupTask<T>
{
    protected final TimeMaster _timeMaster;
    
    protected final AtomicBoolean _shutdown;

    protected long _startupTime;
    
    protected CleanupTask(SharedServiceStuff stuff,
            AtomicBoolean shutdown)
    {
        _timeMaster = stuff.getTimeMaster();
        _shutdown = shutdown;
    }

    protected boolean shouldStop()
    {
        return _shutdown.get();
    }

    protected T cleanUp() throws Exception
    {
        _startupTime = _timeMaster.currentTimeMillis();
        return _cleanUp();
    }

    protected abstract T _cleanUp() throws Exception;

    @Override
    public String toString() {
        return getClass().getName();
    }
}
