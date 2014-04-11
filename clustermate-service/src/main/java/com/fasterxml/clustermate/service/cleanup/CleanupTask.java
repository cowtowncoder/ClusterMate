package com.fasterxml.clustermate.service.cleanup;

import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.storemate.shared.TimeMaster;

public abstract class CleanupTask<T>
{
    protected TimeMaster _timeMaster;
    
    protected AtomicBoolean _shutdown;

    protected long _startupTime;

    protected CleanupTask() { }

    protected void init(SharedServiceStuff stuff, Stores<?,?> stores,
            ClusterViewByServer cluster, AtomicBoolean shutdown)
    {
        _timeMaster = stuff.getTimeMaster();
        _shutdown = shutdown;
    }

    /**
     * Method called by {@link CleanerUpper} when its own <code>prepareForStop()</code>
     * is called; sub-classes may react. Note that when method gets called,
     * {@link #_shutdown} has already been set to <code>true</code>.
     */
    protected void prepareForStop() {
        // Nothing generic to do here
    }
    
    protected boolean shouldStop() {
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
