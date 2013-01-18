package com.fasterxml.clustermate.service.cleanup;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.file.*;

/**
 * {@link CleanupTask} responsible for managing file system structure
 * by pruning empty directories and, if necessary, deleting ancient
 * files that have slipped through basic TTL expiration mechanism.
 */
public class FileCleaner extends CleanupTask<FileCleanupStats>
{
    private final static Logger LOG = LoggerFactory.getLogger(FileCleaner.class);
    
    protected final FileManager _fileManager;

    /**
     * Age used for checking if a directory is safe to completely nuke,
     * due to its contents necessarily being older than their maximum
     * lifetime. This is actually 1 day higher than configured value,
     * to allow for things like clock skew, wrongly configured timezones
     * and alike.
     */
    protected final long _maxTimeToLiveMsecs;
    
    public FileCleaner(SharedServiceStuff stuff, AtomicBoolean shutdown)
    {
        super(stuff, shutdown);
        _fileManager = stuff.getFileManager();
        // let's use max-TTL-plus-one-day 
        _maxTimeToLiveMsecs = stuff.getServiceConfig().cfgMaxMaxTTL.getMillis()
                + new TimeSpan("1d").getMillis();
    }

    @Override
    public FileCleanupStats _cleanUp()
    {
        final FileCleanupStats stats = new FileCleanupStats();

        LOG.info("Starting file cleanup: will nuke any dirs older than {}",
                TimeMaster.timeDesc(_maxTimeToLiveMsecs));
        
        // iterate over all but the last directory; last considered current
        List<DirByDate> dateDirs = _fileManager.listMainDataDirs(stats);
        final int dirCount = dateDirs.size();
        if (dirCount == 0) { // none?
            LOG.warn("No date directories found for clean up -- bailing out");
            return stats;
        }
        for (int i = 0, end = dirCount-1; i < end; ++i) {
            _cleanDateDir(dateDirs.get(i), stats);
        }
        // all done
        LOG.info("Completed file clean up (skipped the last date-dir, '{}'): {}",
                dateDirs.get(dirCount-1).getDirectory().getAbsolutePath(), stats);
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
                LOG.info("Terminating file cleanup pre-maturely");
                return;
            }
            // ok; we know rough time by now...
            long msecsAgo = _timeMaster.currentTimeMillis() - timeDir.getRawCreateTime();
            // ancient?
            if (msecsAgo > _maxTimeToLiveMsecs) {
                int failedFiles = timeDir.nuke(stats);
                if (failedFiles == 0) {
                    ++deleted;
                    continue;
                }
                LOG.warn("Failed to nuke directory {}; {} files remain, must skip",
                        timeDir.toString(), failedFiles);
            } else {
                // otherwise just weed out empty dirs
                if (timeDir.removeEmpty(stats)) {
                    ++deleted;
                    continue;
                }
            }
            stats.addRemainingDir();
            ++remaining;
        }
        // how about date dir itself... empty by now?
        if (remaining == 0) {
            File dir = dateDir.getDirectory();
            if (dir.delete()) {
                if (deleted == 0) { // was already empty
                    stats.addDeletedEmptyDir();
                } else {
                    stats.addDeletedDir();
                }
                return;
            }
            LOG.warn("Failed to nuke directory {} for some reason, must skip", dir);
        }
        stats.addRemainingDir();
    }
}
