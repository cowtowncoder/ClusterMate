package com.fasterxml.clustermate.service.cleanup;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.*;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.LastAccessStore;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;

/**
 * Helper class used to keep track of clean up progress
 * for local BDB cleanup.
 */
public class LocalEntryCleaner<K extends EntryKey, E extends StoredEntry<K>>
    extends CleanupTask<LocalCleanupStats>
{
    private final static Logger LOG = LoggerFactory.getLogger(LocalEntryCleaner.class);

    /**
     * Time-to-live for tomb stones
     */
    protected long _tombstoneTTLMsecs;

    protected StorableStore _entryStore;

    protected LastAccessStore<K, E> _lastAccessStore;

    protected StoredEntryConverter<K,E,?> _entryFactory;
    
    protected boolean _isTesting;
    
    public LocalEntryCleaner() { }
    
    @SuppressWarnings("unchecked")
    @Override
    protected void init(SharedServiceStuff stuff,
            Stores<?,?> stores,
            ClusterViewByServer cluster,
            AtomicBoolean shutdown)
    {
        super.init(stuff, stores, cluster, shutdown);
        _tombstoneTTLMsecs = stuff.getServiceConfig().cfgTombstoneTTL.getMillis();
        _entryFactory = stuff.getEntryConverter();
        _entryStore = stores.getEntryStore();
        _lastAccessStore = (LastAccessStore<K, E>) stores.getLastAccessStore();
        _isTesting = stuff.isRunningTests();
    }
    
    @Override
    protected LocalCleanupStats _cleanUp() throws Exception
    {
        final LocalCleanupStats stats = new LocalCleanupStats();

        if (_entryStore.isClosed()) {
            if (!_isTesting) {
                LOG.warn("LocalEntryCleanup task cancelled: Entry DB has been closed");
            }
            return stats;
        }
        
        final long tombstoneThreshold = _timeMaster.currentTimeMillis() - _tombstoneTTLMsecs;
        _entryStore.iterateEntriesByModifiedTime(new StorableLastModIterationCallback() {
            @Override
            public IterationAction verifyTimestamp(long timestamp) {
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction verifyKey(StorableKey key)
            {
                // first things first: do we need to quit?
                // TODO: maybe consider max runtime?
                if (shouldStop()) {
                    return IterationAction.TERMINATE_ITERATION;
                }
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction processEntry(Storable raw) throws StoreException
            {
                // for tombstones easy, common max-TTL:
                final StoredEntry<K> entry = _entryFactory.entryFromStorable(raw);
                if (raw.isDeleted()) {
                    if (entry.insertedBefore(tombstoneThreshold)) {
                        delete(raw.getKey());
                        stats.addExpiredTombstone();
                    } else {
                        stats.addRemainingTombstone();
                    }
                    return IterationAction.PROCESS_ENTRY;
                }
                // for other entries bit more complex; basically checking following possibilities:
                // (a) Entry is older than its maxTTL (which varies entry by entry), can be removed
                // (b) Entry is younger than its minTTL since creation, can be skipped
                // (c) Entry does not use access-time: remove
                // (d) Entry needs to be retained based on local last-access time: skip
                // (e) Must check global last-access to determine whether to keep or skip
                final long currentTime = _timeMaster.currentTimeMillis();
                if (entry.hasExceededMaxTTL(currentTime)) { // (a) remove
                    stats.addExpiredMaxTTLEntry();
                    delete(raw.getKey());
                } else if (!entry.hasExceededMinTTL(currentTime)) { // (b) skip
                    stats.addRemainingEntry();
                } else if (!entry.usesLastAccessTime()) { // (c) remove
                    stats.addExpiredLastAccessEntry();
                    delete(raw.getKey());
                } else if (!entry.hasExceededLastAccessTTL(currentTime,
                        _lastAccessStore.findLastAccessTime(entry.getKey(), entry.getLastAccessUpdateMethod()))) {
                    stats.addRemainingEntry(); // (d) keep
                } else { // (e): add to list of things to check...
                    // !!! TODO
                    stats.addRemainingEntry();
                }
                return IterationAction.PROCESS_ENTRY;
            }

            private void delete(StorableKey key) throws StoreException
            {
                try {
                    _entryStore.hardDelete(key, true);
                } catch (StoreException e) {
                    throw e;
                } catch (IOException e) {
                    throw new StoreException.IO(key, e);
                }
            }
        
        }, 0L);
        
        return stats;
    }
}
