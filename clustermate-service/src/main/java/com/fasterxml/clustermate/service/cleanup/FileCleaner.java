package com.fasterxml.clustermate.service.cleanup;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.file.*;

/**
 * {@link CleanupTask} responsible for managing file system structure
 * by pruning empty directories and, if necessary, deleting ancient
 * files that have slipped through basic TTL expiration mechanism.
 */
public class FileCleaner extends CleanupTask<FileCleanupStats>
{
    private final Logger LOG;
    
    protected FileManager _fileManager;

    /**
     * Age used for checking if a directory is safe to completely nuke,
     * due to its contents necessarily being older than their maximum
     * lifetime. This is actually 1 day higher than configured value,
     * to allow for things like clock skew, wrongly configured timezones
     * and alike.
     */
    protected long _maxTimeToLiveMsecs;
    
    public FileCleaner() {
        this(LoggerFactory.getLogger(FileCleaner.class));
    }

    public FileCleaner(Logger log) {
        LOG = log;
    }
    
    @Override
    protected void init(SharedServiceStuff stuff,
            Stores<?,?> stores,
            ClusterViewByServer cluster,
            AtomicBoolean shutdown)
    {
        super.init(stuff, stores, cluster, shutdown);
        _fileManager = stuff.getFileManager();
        // let's use max-TTL-plus-one-day 
        _maxTimeToLiveMsecs = stuff.getServiceConfig().cfgMaxMaxTTL.getMillis()
                + new TimeSpan("1d").getMillis();
    }
    
    @Override
    public FileCleanupStats _cleanUp()
    {
        final FileCleanupStats stats = new FileCleanupStats();
        _reportStart();

        // iterate over all but the last directory; last considered current
        List<DirByDate> dateDirs = _fileManager.listMainDataDirs(stats);
        final int dirCount = dateDirs.size();
        if (dirCount == 0) { // none?
            _reportEndNoDirs();
            return stats;
        }
        for (int i = 0, end = dirCount-1; i < end; ++i) {
            _cleanDateDir(dateDirs.get(i), stats);
        }
        // all done
        _reportEndSuccess(stats, dateDirs.get(dirCount-1).getDirectory());
        return stats;
    }

    /**
     * Helper method called for given "date directory", to do recursively clean up.
     */
    protected void _cleanDateDir(DirByDate dateDir, FileCleanupStats stats)
    {
        int remaining = 0, deleted = 0;
        for (DirByTime timeDir : dateDir.listTimeDirs(stats)) {
            if (shouldStop()) {
                _reportProblem("Terminating file cleanup pre-maturely, due to stop request");
                return;
            }
            // ok; we know rough time by now...
            long msecsAgo = _timeMaster.currentTimeMillis() - timeDir.getRawCreateTime();
            // ancient?
            if (msecsAgo > _maxTimeToLiveMsecs) {
                int failedFiles = timeDir.nuke(stats, _shutdown);
                if (failedFiles == 0) {
                    ++deleted;
                    continue;
                }
                _reportProblem("Failed to nuke directory "+timeDir.toString()+"; "+failedFiles+" files remain, must skip");
            } else {
                // otherwise just weed out empty dirs
                if (timeDir.removeEmpty(stats, _shutdown)) {
                    ++deleted;
                    continue;
                }
            }
            stats.addRemainingDir();
            ++remaining;
        }
        // how about date dir itself... empty by now?
        if (remaining == 0 && !_shutdown.get()) {
            File dir = dateDir.getDirectory();
            if (dir.delete()) {
                if (deleted == 0) { // was already empty
                    stats.addDeletedEmptyDir();
                } else {
                    stats.addDeletedDir();
                }
                return;
            }
            _reportProblem("Failed to nuke directory "+dir+" for some reason, must skip");
        }
        stats.addRemainingDir();
    }

    /*
    /**********************************************************************
    /* Overridable reporting methods
    /**********************************************************************
     */

    protected void _reportStart()
    {
        if (LOG != null) {
            LOG.info("Starting file cleanup: will nuke any dirs older than {}",
                    TimeMaster.timeDesc(_maxTimeToLiveMsecs));
        }
    }

    protected void _reportProblem(String msg)
    {
        if (LOG != null) {
            LOG.warn(msg);
        }
    }
    
    protected void _reportEndNoDirs() {
        if (LOG != null) {
            LOG.warn("No date directories found for clean up -- bailing out");
        }
    }

    protected void _reportEndSuccess(FileCleanupStats stats, File skippedDir)
    {
        if (LOG != null) {
            LOG.info("Completed file clean up (skipped the last date-dir, '{}'): {}",
                skippedDir.getAbsolutePath(), stats);
        }
    }
}
