package com.fasterxml.clustermate.service.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.state.NodeStateStore;
import com.fasterxml.storemate.store.util.BoundedInputStream;
import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.sync.*;
import com.fasterxml.clustermate.service.util.StoreUtil;

public class ClusterPeerImpl<K extends EntryKey, E extends StoredEntry<K>>
    extends ClusterPeer
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    /**
     * If access to peer's sync/list fails, wait for this duration before
     * trying again.
     */
    private final static long SLEEP_FOR_SYNCLIST_ERRORS_MSECS = 10000L;

    private final static long SLEEP_FOR_SYNCPULL_ERRORS_MSECS = 3000L;

    // no point trying to sleep for trivial time
    private final static long MINIMAL_SLEEP_MSECS = 10L;
    
    /**
     * If synclist is empty and server does not instruct us, simply sleep for
     * 1 second (to try avoid congestion)
     */
    private final static long SLEEP_FOR_EMPTY_SYNCLIST_MSECS = 1000L;
    
    // no real hurry; use 20 seconds to account for GC, congestion etc
    private final static TimeSpan TIMEOUT_FOR_SYNCLIST = new TimeSpan(10L, TimeUnit.SECONDS);

    /**
     * Lowish timeout for "bye bye" message, so it won't block shutdown
     */
    private final static TimeSpan TIMEOUT_FOR_BYEBYE = new TimeSpan(250L, TimeUnit.MILLISECONDS);
    
    /**
     * We will limit maximum estimate response size to some reasonable
     * limit: starting with 250 megs. The idea is to use big enough sizes
     * for efficient bulk transfer; but small enough not to cause timeouts
     * during normal operation.
     */
    private final static long MAX_TOTAL_PAYLOAD = 250 * 1000 * 1000;
    
    /**
     * During fetching of items to sync, let's cap number of failures to some
     * number; this should make it easier to recover from cases where peer
     * shuts down during individual sync operation (after sync list received
     * but before all entries are fetched)
     */
    private final static int MAX_SYNC_FAILURES = 8;

    /**
     * Also, let's limit maximum individual calls per sync-pull fetch portion,
     * to avoid excessive calls.
     */
    private final int MAX_FETCH_TRIES = 20;

    private final static Logger LOG = LoggerFactory.getLogger(ClusterPeer.class);
    
    /*
    /**********************************************************************
    /* Configuration, general helpers
    /**********************************************************************
     */

    protected final SharedServiceStuff _stuff;

    /**
     * This object is necessary to support "virtual time" for test cases.
     */
    protected final TimeMaster _timeMaster;

    /*
    /**********************************************************************
    /* Operation state of the peer object
    /**********************************************************************
     */
    
    /**
     * Synchronization thread if (and only if) this peer shares part of keyspace
     * with the local node; otherwise null.
     * Note that threads may be started and stopped based on changes to cluster
     * configuration.
     */
    protected Thread _syncThread;

    /**
     * Flag used to request termination of the sync thread.
     */
    protected AtomicBoolean _running = new AtomicBoolean(false);

    /**
     * Let's keep track of number of failures (as per caught exceptions); mostly
     * so that tests can verify passing, but also potentially for monitoring.
     */
    protected AtomicInteger _failCount = new AtomicInteger(0);
    
    /*
    /**********************************************************************
    /* Helper objects for entry handling
    /**********************************************************************
     */

    /**
     * And to store fetched missing entities, we need the store
     */
    protected final StorableStore _entryStore;

    /**
     * Need to construct metadata nuggets with this factory
     */
    protected final StoredEntryConverter<K,E,?> _entryConverter;
    
    /*
    /**********************************************************************
    /* Helper objects for sync handling
    /**********************************************************************
     */

    protected final ClusterViewByServerUpdatable _cluster;
    
    /**
     * Object used to access Node State information, needed to construct
     * view of the cluster.
     */
    protected final ClusterStatusAccessor _statusAccessor;

    /**
     * Helper object used for doing HTTP requests
     */
    protected final SyncListAccessor _syncListAccessor;

    /**
     * Persistent data store in which we store information regarding
     * synchronization.
     */
    protected final NodeStateStore<IpAndPort, ActiveNodeState> _stateStore;

    /**
     * Hash code of contents of the last cluster view we received from
     * the peer. Used as optimization: cluster view only piggy-backed
     * on list response if hash differs.
     */
    protected long _lastClusterHash;
    
    /*
    /**********************************************************************
    /* Local information for peer (which for us is external but...)
    /**********************************************************************
     */

    /**
     * Synchronization state of this peer
     */
    protected ActiveNodeState _syncState;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public ClusterPeerImpl(SharedServiceStuff stuff, ClusterViewByServerUpdatable cluster,
            NodeStateStore<IpAndPort, ActiveNodeState> stateStore, StorableStore entryStore,
            ActiveNodeState state,
            ClusterStatusAccessor accessor)
    {
        super();
        _cluster = cluster;
        _stuff = stuff;
        _syncListAccessor = new SyncListAccessor(stuff);
        _syncState = state;
        _stateStore = stateStore;
        _entryStore = entryStore;
        _timeMaster = stuff.getTimeMaster();
        _entryConverter = stuff.getEntryConverter();
        _statusAccessor = accessor;
    }

    @Override
    public void start() {
        startSyncing();
    }

    /**
     * If we get notified about a shutdown in future, we can take some
     * preparatory steps, to make eventual shutdown simpler, safer
     * and quicker.
     */
    @Override
    public void prepareForStop() {
        _stop(false);
    }
    
    /**
     * Method called when the system is shutting down.
     */
    @Override
    public void stop() {
        _stop(true);
    }

    protected void _stop(boolean forced)
    {
        // stopSyncing():
        Thread t;
        synchronized (this) {
            _running.set(false);
            t = _syncThread;
            if (t != null) {
                _syncThread = null;
                LOG.info("Stop requested for sync thread for peer at {}", _syncState.getAddress());
            }
        }
        if (t != null) {
//            t.notify();
            t.interrupt();
        }
        _syncListAccessor.stop();
    }

    /*
    /**********************************************************************
    /* Actual synchronization task
    /**********************************************************************
     */
    
    /**
     * Method that can be called to ensure that there is a synchronization
     * thread running to sync between the local node and this peer
     * 
     * @return True if a new sync thread was started; false if there already 
     *    was a thread
     */
    public boolean startSyncing()
    {
        Thread t;
        
        synchronized (this) {
            t = _syncThread;
            if (t != null) { // sanity check
                return false;
            }
            _running.set(true);
            _syncThread = t = new Thread(new Runnable() {
                @Override
                public void run() {
                    syncLoop();
                }
            });
            _syncThread.setDaemon(true);
            _syncThread.setName("NodeSync-"+_syncState.getAddress());
        }
        t.start();
        return true;
    }
    
    /*
    /**********************************************************************
    /* State access
    /**********************************************************************
     */

    @Override
    public int getFailCount() { return _failCount.get(); }

    @Override
    public void resetFailCount() { _failCount.set(0); }

    @Override
    public long getSyncedUpTo() {
        return _syncState.getSyncedUpTo();
    }

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */

    @Override
    public IpAndPort getAddress() {
        return _syncState.getAddress();
    }

    @Override
    public KeyRange getActiveRange() {
        return _syncState.getRangeActive();
    }

    @Override
    public KeyRange getTotalRange() {
        return _syncState.totalRange();
    }

    /**
     * Accessor for getting key range that is shared between the local node
     * and this peer; for non-overlapping nodes this may be an empty range.
     */
    @Override
    public KeyRange getSyncRange() {
        return _syncState.getRangeSync();
    }

    /*
    /**********************************************************************
    /* Extended accessors
    /**********************************************************************
     */

    public ActiveNodeState getSyncState() {
        return _syncState;
    }

    public boolean isDisabled() {
        return _syncState.isDisabled();
    }
    
    /*
    /**********************************************************************
    /* Background synchronization processing
    /**********************************************************************
     */

    /**
     * Main synchronization loop
     */
    protected void syncLoop()
    {
        LOG.info("Starting sync thread for peer at {}", _syncState.getAddress());
        
        // For testing (and only testing!), let's add little bit of
        // virtual sleep (TimeMaster will block threads) before starting
        // the loop; this to stabilize situation
        if (_stuff.isRunningTests()) {
            try {
                _timeMaster.sleep(1L);
            } catch (InterruptedException e) { }
        }

        /* At high level, we have two kinds of tasks, depending on whether
         * there is any overlap:
         * 
         * 1. If ranges overlap, we need to do proper sync list/pull handling
         * 2. If no overlap, we just need to keep an eye towards changes, to
         *   try to keep whole cluster view up to date (since clients need it)
         */
        while (_running.get()) {
            try {
                if (hasOverlap(_cluster.getLocalState(), _syncState)) {
                    doRealSync();
                } else {
                    doMinimalSync();
                }
            } catch (InterruptedException e) {
                if (_running.get()) {
                    LOG.warn("syncLoop() interrupted without clearing '_running' flag; ignoring");
                }
            } catch (Exception e) {
                LOG.warn("Uncaught processing exception during syncLoop(): ({}) {}",
                        e.getClass().getName(), e.getMessage());
                if (_running.get()) {
                    // Ignore failures during shutdown, so only increase here
                    _failCount.addAndGet(1);
                    try {
                        _timeMaster.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
                    } catch (InterruptedException e2) { }
                }
            }
        }
        if (_stuff.isRunningTests()) {
            LOG.info("Stopped sync thread for peer at {} -- testing, all done!", _syncState.getAddress());
            return;
        }
        // And send the byebye message if peer is NOT (known to be) disabled
        if (_syncState.isDisabled()) {
            LOG.info("Stopped sync thread for peer at {}: is disabled, no need to send bye-bye", _syncState.getAddress());
            return;
        }
        LOG.info("Stopped sync thread for peer at {}: let's send bye-bye", _syncState.getAddress());
        long start = System.currentTimeMillis();
        _syncListAccessor.sendStatusUpdate(_cluster, TIMEOUT_FOR_BYEBYE,
                _syncState.getAddress(), ClusterMateConstants.STATE_INACTIVE);
        LOG.info("Bye-bye message to {} sent in {} msec", _syncState.getAddress(), System.currentTimeMillis()-start);
    }

    protected void doRealSync() throws Exception
    {
        /* Sequence for each iterations consists of:
         * 
         * 1. Fetch list of newly inserted/deleted entries from peer (sync/list)
         * 2a. Find subset of entries unknown to this node, if any
         * 2b. Fetch unknown entries, possibly with multiple requests
         * 
         * and we will also add bit of sleep between requests, depending on how many
         * entries we get in step 1.
         */
        
        long listTime = _timeMaster.currentTimeMillis();
        SyncListResponse<?> syncResp = _fetchSyncList();
        if (!_running.get()) { // short-circuit during shutdown
            return;
        }
        if (syncResp == null) { // only for hard errors
            _timeMaster.sleep(SLEEP_FOR_SYNCLIST_ERRORS_MSECS);
            return;
        }

        // First things first:
        if (syncResp.clusterStatus != null) {
            _cluster.updateWith(syncResp.clusterStatus);
            _lastClusterHash = syncResp.clusterHash;
        } else {
            // This is fine, as long as hashes match
            if (syncResp.clusterHash != _lastClusterHash) {
                LOG.warn("Did not get cluster status from {} even though hashes differ 0x{} (old) vs 0x{} (response)",
                        _syncState.getAddress(),
                        Long.toHexString(_lastClusterHash), Long.toHexString(syncResp.clusterHash));
            }
        }
        final long lastSeenTimestamp = syncResp.lastSeen();
        // Sanity check; should never happen
        if (lastSeenTimestamp <= 0L) {
            LOG.error("Invalid lastSeen timestamp value {} for SyncList, from {}",
                    lastSeenTimestamp, _syncState.getAddress());
            // should we sleep bit extra? Probably good to avoid flooding logs, if we end up here
            Thread.sleep(100L);
        }

        // comment out or remove for production; left here during testing:
//long diff = (listTime - syncResp.lastSeen()) >> 10; // in seconds
//LOG.warn("Received syncList with {} responses; last timestamp {} secs ago", syncResp.size(), diff);
        
        List<SyncListResponseEntry> newEntries = syncResp.entries;
        int insertedEntryCount = newEntries.size();
        if (insertedEntryCount == 0) { // nothing to update
            // may still need to update timestamp?
            _updatePersistentState(listTime, lastSeenTimestamp);

            // Ok: maybe server instructed us as to how long to sleep?
            long sleepMsecs = syncResp.clientWait;
            if (sleepMsecs < MINIMAL_SLEEP_MSECS) { // if not, use some lowish default amount
                sleepMsecs = SLEEP_FOR_EMPTY_SYNCLIST_MSECS;
            }
//            long timeSpent = _timeMaster.currentTimeMillis() - listTime;
            _timeMaster.sleep(sleepMsecs);
            return;
        }
        // Ok, we got something, good.
        // First: handle tombstones we may be getting:
        @SuppressWarnings("unused")
        int tombstoneCount = _handleTombstones(newEntries);
        // then filter out entries that we already have:
        _filterSeen(newEntries);
        if (!_running.get()) { // short-circuit during shutdown
            return;
        }
        if (newEntries.isEmpty()) { // nope: just update state then
            /*
            long msecs = syncResp.lastSeen() - _syncState.syncedUpTo;
            if (!_stuff.isRunningTests()) {
                LOG.warn("No unseen entries out of {} entries: timestamp = {} (+{} sec)",
                    new Object[] { insertedEntryCount, syncResp.lastSeen(), String.format("%.1f", msecs/1000.0)});
            }
            */
            _updatePersistentState(listTime, lastSeenTimestamp);
        } else { // yes: need to do batch updates
            // but can at least update syncUpTo to first entry, right?
            int newCount = newEntries.size();
            AtomicInteger rounds = new AtomicInteger(0);
            long lastProcessed = _fetchMissing(newEntries, rounds);
            int fetched = newCount - newEntries.size();

            double secs = (_timeMaster.currentTimeMillis() - listTime) / 1000.0;
            String timeDesc = String.format("%.2f", secs);
            LOG.info("Fetched {}/{} missing entries from {} in {} seconds ({} rounds)",
                    new Object[] { fetched, newCount, getAddress(), timeDesc, rounds.get()});
            _updatePersistentState(listTime, lastProcessed);
        }
        // And then sleep a bit, before doing next round of syncing
        long msecsBehind = (_timeMaster.currentTimeMillis() - _syncState.getSyncedUpTo());
        long delay = _calculateSleepBetweenSync(insertedEntryCount, msecsBehind);
        
        if (delay > 0L) {
            // only bother informing if above 50 msec sleep
            if (delay >= 50L) {
                double secsBehind = delay / 1000.0;
                LOG.info("With {} listed entries, {} seconds behind, will do {} msec sleep",
                        new Object[] { insertedEntryCount, String.format("%.2f", secsBehind), delay});
            }
            _timeMaster.sleep(delay);
        }
    }

    /**
     * Method called when there is no key range overlap, and at most we want to
     * synchronize cluster view occasionally.
     */
    protected void doMinimalSync() throws Exception
    {
        LOG.info("doMinimalSync(): let's just... Sleep for a bit (TBD)");
        Thread.sleep(30 * 1000L);
        
        // !!! TODO: do something!
    }
    
    /*
    /**********************************************************************
    /* Internal methods, cluster state updates
    /**********************************************************************
     */

    /**
     * Method called to indicate that the node should (or should not) be
     * disabled.
     */
    public void markDisabled(long timestamp, boolean isDisabled)
    {
        if (timestamp <= 0L) { // optional
            timestamp = _syncState.getDisabledUpdated();
        }
        ActiveNodeState state = _syncState.withDisabled(timestamp, isDisabled);
        if (state != _syncState) {
            _syncState = state;
            try {
                _stateStore.upsertEntry(state.getAddress(), state);
            } catch (Exception e) {
                LOG.error("Failed to update node state (disabled to {}) for {}. Problem ({}): {}",
                        isDisabled, _syncState, e.getClass().getName(), e.getMessage());
            }
        }
    }
    
    /*
    /**********************************************************************
    /* Internal methods, other
    /**********************************************************************
     */
    
    /**
     * Helper method called to update persistent state, based on sync list
     * information.
     * 
     * @param syncStartTime Timestamp of when sync attempt was done
     * @param lastSeen
     */
    private void _updatePersistentState(long syncStartTime, long lastSeen)
    {
        ActiveNodeState orig = _syncState;
        _syncState = _syncState.withLastSyncAttempt(syncStartTime);
        if (lastSeen > _syncState.getSyncedUpTo()) {
            _syncState = _syncState.withSyncedUpTo(lastSeen);
        }
        if (_syncState != orig) {
//LOG.warn("Saving sync state ({}) (args: {}, {}): lastStartTime {}, lastSeen {}", orig.address, syncStartTime, lastSeen, _syncState.lastSyncAttempt, _syncState.syncedUpTo);
            try {
                _stateStore.upsertEntry(_syncState.getAddress(), _syncState);
            } catch (Exception e) {
                LOG.error("Failed to update node state for {}. Problem ({}): {}",
                        _syncState, e.getClass().getName(), e.getMessage());
            }
        }
    }
    
    private SyncListResponse<?> _fetchSyncList() throws InterruptedException
    {
        try {
            return _syncListAccessor.fetchSyncList(_cluster,
                    TIMEOUT_FOR_SYNCLIST, _syncState, _lastClusterHash);
        } catch (InterruptedException e) {
            // no point in complaining if we are being shut down:
            if (_running.get()) {
                LOG.warn("Failed to fetch syncList from {} ({}): {}",
                        new Object[] { _syncState.getAddress(), e.getClass().getName(), e.getMessage()});
            }
        }
        return null;
    }

    /**
     * Helper method called to handle removal of entries, by handling
     * tombstones received and converting existing non-deleted local
     * entries into tombstones.
     * 
     * @return Number of tombstone entries found on the list
     */
    protected int _handleTombstones(List<SyncListResponseEntry> entries)
        throws IOException, StoreException
    {
        int count = 0;
        Iterator<SyncListResponseEntry> it = entries.iterator();
        while (it.hasNext()) {
            SyncListResponseEntry entry = it.next();
            // Tombstone: if we have an entry, convert to a tombstone.
            /* 06-Jul-2012, tatu: But if we don't have one, should we create one?
             *   Could think of it either way; but for now, let's not waste time and space
             */
            if (entry.deleted()) {
                ++count;
                it.remove();
                _entryStore.softDelete(StoreOperationSource.SYNC, null, entry.key, true, true);
            }
        }
        return count;
    }

    protected void _filterSeen(List<SyncListResponseEntry> entries)
        throws IOException, StoreException
    {
        Iterator<SyncListResponseEntry> it = entries.iterator();
        while (it.hasNext()) {
            SyncListResponseEntry remoteEntry = it.next();
            /* 11-Jun-2013, tatu: Although tombstones have been handled and removed,
             *   need to pay attention here, since conflict resolution may be
             *   necessary.
             */
            Storable localEntry = _entryStore.findEntry(StoreOperationSource.SYNC, null, remoteEntry.key);
            if (localEntry != null) {
                // Do we have an actual conflict? If so, needs resolution as per:
                if (StoreUtil.needToPullRemoteToResolve(localEntry.getLastModified(), localEntry.getContentHash(),
                        remoteEntry.insertionTime, remoteEntry.hash)) {
                    continue;
                }
                it.remove();
            }
        }
    }
    
    /**
     * Helper method that handles actual fetching of missing entries, to synchronize
     * content.
     * 
     * @param missingEntries Entries to try to fetch
     * @rounds Integer to update with number of rounds done to sync things completely
     * 
     * @return Timestamp to use as the new 'syncedUpTo' value
     */
    private long _fetchMissing(List<SyncListResponseEntry> missingEntries, AtomicInteger rounds)
        throws InterruptedException
    {
        // initially create as big batches as possible
        int maxToFetch = missingEntries.size();
        int tries = 0;
        int fails = 0;
        long syncedUpTo = 0L;

        do {
            ++tries;
            final long startTime = _timeMaster.currentTimeMillis();
            AtomicInteger payloadSize = new AtomicInteger(0);
            SyncPullRequest req = _buildSyncPullRequest(missingEntries, maxToFetch, payloadSize);
            final int expCount = req.size();

            if (expCount == 0) { // sanity check, shouldn't happen but...
                throw new IllegalStateException("Internal error: empty syncPullRequest list ("+missingEntries.size()+" missing entries)");
            }

            rounds.addAndGet(1);
            AtomicInteger status = new AtomicInteger(0);
            InputStream in = null;
            try {
                in = _syncListAccessor.readSyncPullResponse(req, TIMEOUT_FOR_SYNCLIST,
                         getAddress(), status, payloadSize.get());
//            } catch (org.apache.http.conn.HttpHostConnectException e) { // if using Apache HC
            } catch (java.net.ConnectException e) {
                ++fails;
                LOG.warn("Failed to connect server "+getAddress()+" to fetch missing entries", e);
                _timeMaster.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
            } catch (Exception e) {
                LOG.warn("Problem trying to make syncPull call to fetch "+expCount+" entries: ("
                        +e.getClass().getName() + ") " + e.getMessage(), e);
                ++fails;
                _timeMaster.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
            }
            if (in == null) {
                LOG.warn("Problem trying to fetch {} entries, received status code of {}",
                        expCount, status.get());
                _timeMaster.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
                ++fails;
                continue;
            }

            Iterator<SyncListResponseEntry> it = missingEntries.iterator();
            int count = 0;
            int headerLength = 0;
            long payloadLength = 0;
            try {
                // let's see if we can correlate entries nicely
                headerLength = -1;
                payloadLength = -1;
                for (; it.hasNext(); ++count, it.remove()) {
                    SyncListResponseEntry reqEntry = it.next();
                    headerLength = SyncPullResponse.readHeaderLength(in);
                    // Service will indicate end-of-response with marker length
                    if (headerLength == SyncHandler.LENGTH_EOF) {
                        break;
                    }
                    // sanity check:
                    if (count == expCount) {
                        LOG.warn("Server returned more than expected {} entries; ignoring rest!", expCount);
                        break;
                    }
                    // missing header? Unexpected, but not illegal
                    if (headerLength == 0) {
                        LOG.warn("Missing entry {}/{} (from {}), id {}: expired?",
                                new Object[] { count, expCount, _syncState.getAddress(), reqEntry.key});
                        continue;
                    }
                    
                    byte[] headerBytes = new byte[headerLength];
                    int len = IOUtil.readFully(in, headerBytes);
                    if (len < headerLength) {
                        throw new IOException("Unexpected end-of-input: got "+len+" bytes; needed "+headerLength);
                    }
                    SyncPullEntry header = _syncListAccessor.decodePullEntry(headerBytes);
                    payloadLength = header.storageSize;
                    // and then create the actual entry:
                    _pullEntry(reqEntry, header, in);
                    syncedUpTo = reqEntry.insertionTime; 
                }
                if (count < expCount) {
                    // let's consider 0 entries to be an error, to prevent infinite loops
                    if (count == 0) {
                        LOG.warn("Server returned NO entries, when requested {}", expCount);
                        ++fails;
                    }
                    LOG.warn("Server returned fewer entries than requested for sync pull: {} vs {} (in {} msecs)",
                            new Object[] { count, expCount, (_timeMaster.currentTimeMillis() - startTime)});
                }
            } catch (Exception e) {
                LOG.warn("Problem trying to fetch syncPull entry {}/{} (header-length: {}, length: {}): ({}) {}",
                        new Object[] { count+1, expCount, headerLength, payloadLength, e.getClass().getName(), e.getMessage() } );
                _timeMaster.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
                ++fails;
            } finally {
                if (in != null) {
                    try { in.close(); } catch (Exception e) { // shouldn't really happen
                        LOG.warn("Failed to close HTTP stream: {}", e.getMessage());
                    }
                }
            }
        } while (fails < MAX_SYNC_FAILURES && !missingEntries.isEmpty() && tries < MAX_FETCH_TRIES);

        if (fails > 0) {
            _failCount.addAndGet(fails);
        }
        
        return syncedUpTo;
    }

    private SyncPullRequest _buildSyncPullRequest(List<SyncListResponseEntry> missingEntries,
            int maxEntries, AtomicInteger expectedPayloadSize)
    {
        SyncPullRequest req = new SyncPullRequest();
        Iterator<SyncListResponseEntry> it = missingEntries.iterator();
        SyncListResponseEntry entry = it.next();
        req.addEntry(entry.key);
        long expSize = entry.size;
        while (it.hasNext() && req.size() < maxEntries) {
            entry = it.next();
            expSize += entry.size;
            if (expSize > MAX_TOTAL_PAYLOAD) {
                expSize -= entry.size;
                break;
            }
            req.addEntry(entry.key);
        }
        expectedPayloadSize.set((int) expSize);
        return req;
    }
    
    /**
     * Helper method called to figure out how long to sleep before doing next syncList call.
     * Note that sleep times are rather arbitrary: we hope to be able to better
     * tune these in future.
     * 
     * @param listedCount number of 'newly inserted' entries that were returned
     * @param Number of second that we are "behind" current time (note: due to grace period,
     *    will never be zero, but more like a minute or so at minimum)
     */
    private long _calculateSleepBetweenSync(int listedCount, long msecsBehind)
    {
        // if we are behind by more than 3 minutes, or get >75% "full" response, no delay:
        final int FULL_ENOUGH = _stuff.getServiceConfig().cfgMaxEntriesPerSyncList * 3 / 4;
        if ((msecsBehind >= (3 * 60 * 1000)) || (listedCount >= FULL_ENOUGH)) {
            return 0L;
        }
        // otherwise moderate delay; bit longer for shorter lists
        if (listedCount < 5) {
            return 300L;
        }
        if (listedCount <= 10) {
            return 200L;
        }
        if (listedCount < 40) {
            return 100L;
        }
        return 50L;
    }
    
    /**
     * Method that does the heavy lifting of pulling a single synchronized entry,
     * if and as necessary.
     */
    private void _pullEntry(SyncListResponseEntry reqEntry, SyncPullEntry header,
            InputStream in)
        throws IOException
    {
        final StorableKey key = header.key;

        /* first things first: either read things in memory (for inline inclusion),
         * or pipe into a file.
         */
        long expSize = header.storageSize;
        // Sanity check: although rare, deletion could have occurred after we got
        // the initial sync list, so:
        if (header.isDeleted) {
            _entryStore.softDelete(StoreOperationSource.SYNC, null, key, true, true);
            return;
        }
        StorableCreationResult result;
        StorableCreationMetadata stdMetadata = new StorableCreationMetadata(header.compression,
                header.checksum, header.checksumForCompressed);
        stdMetadata.uncompressedSize = header.size;
        stdMetadata.storageSize = header.storageSize;
        ByteContainer customMetadata = _entryConverter.createMetadata(_timeMaster.currentTimeMillis(),
                header.lastAccessMethod, header.minTTLSecs, header.maxTTLSecs);

        // although not 100% required, we can simplify handling of smallest entries
        if (expSize <= _stuff.getServiceConfig().storeConfig.maxInlinedStorageSize) { // inlineable
            ByteContainer data;

            if (expSize == 0) {
                data = ByteContainer.emptyContainer();
            } else {
                byte[] bytes = new byte[(int) expSize];
                int len = IOUtil.readFully(in, bytes);
                if (len < expSize) {
                    throw new IOException("Unexpected end-of-input: got "+len+" bytes; needed "+expSize);
                }
                data = ByteContainer.simple(bytes);
            }
            // 19-Sep-2013, tatu: May need to upsert, when resolving conflicts
            result = _entryStore.upsertConditionally(StoreOperationSource.SYNC, null, key, data,
                    stdMetadata, customMetadata, true,
                    new ConflictOverwriteChecker(reqEntry.insertionTime));
        } else {
            /* 21-Sep-2012, tatu: Important -- we must ensure that store only reads
             *   bytes that belong to the entry payload. The easiest way is by adding
             *   a wrapper stream that ensures this...
             */
            BoundedInputStream bin = new BoundedInputStream(in, stdMetadata.storageSize, false);
            // 19-Sep-2013, tatu: May need to upsert, when resolving conflicts
            result = _entryStore.upsertConditionally(StoreOperationSource.SYNC, null, key, bin,
                    stdMetadata, customMetadata, true,
                    new ConflictOverwriteChecker(reqEntry.insertionTime));

            if (result.succeeded() && !bin.isCompletelyRead()) { // error or warning?
                Storable entry = result.getNewEntry();
                long ssize = (entry == null) ? -1L : entry.getStorageLength();
                LOG.warn("Problems with sync-pull for '{}': read {} bytes, should have read {} more; entry storageSize: {}",
                        new Object[] { header.key, bin.bytesRead(), bin.bytesLeft(), ssize });
            }
        }
        
        // should we care whether this was redundant or not?
        if (!result.succeeded()) {
            if (result.getPreviousEntry() != null) {
                // most likely ok: already had the entry
                LOG.info("Redundant sync-pull for '{}': entry already existed locally", header.key);
            } else {
                // should this add to 'failCount'? For now, don't
                LOG.warn("Failed sync-pull for '{}': no old entry. Strange!", header.key);
            }
        }
    }

    protected final boolean hasOverlap(NodeState state1, NodeState state2)
    {
        return state1.totalRange().overlapsWith(state2.totalRange());
    }
}
