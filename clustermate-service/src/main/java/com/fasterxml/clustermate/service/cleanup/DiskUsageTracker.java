package com.fasterxml.clustermate.service.cleanup;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;

/**
 * Bogus "cleaner" task that will calculate disk space usage for backend data store.
 */
public class DiskUsageTracker extends CleanupTask<DiskUsageStats>
{
    /* Let's cap max files and dirs traversed, to limit amount of time we
     * spend on collecting stats in case we get misconfigured.
     */
    protected final static int MAX_DIRS = 4000;

    protected final static int MAX_FILES = 25000;
    
    protected final Logger LOG;
    
    /**
     * We will also calculate disk usage of metadata database, if available.
     */
    protected File _dbRoot;

    public DiskUsageTracker() { this(null); }

    public DiskUsageTracker(Logger log) {
        LOG = (log == null) ? LoggerFactory.getLogger(getClass()) : log;
    }

    @Override
    protected void init(SharedServiceStuff stuff, Stores<?,?> stores,
            ClusterViewByServer cluster, AtomicBoolean shutdown)
    {
        super.init(stuff, stores, cluster, shutdown);
        _dbRoot = stores.getEntryStore().getBackend().getStorageDirectory();
    }

    @Override
    public DiskUsageStats _cleanUp()
    {
        DiskUsageStats stats = new DiskUsageStats(MAX_DIRS, MAX_FILES);
        if (_dbRoot != null) {
            _add(stats, _dbRoot);
        }
        return stats;
    }

    private boolean _add(DiskUsageStats stats, File curr)
    {
        if (curr.isDirectory()) {
            if (!stats.addDir()) {
                return false;
            }
            for (File f : curr.listFiles()) {
                if (!_add(stats, f)) {
                    return false;
                }
            }
        } else {
            if (!stats.addFile(curr)) {
                return false;
            }
        }
        return true;
    }
}
