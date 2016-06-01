package com.fasterxml.clustermate.service.remote;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.util.BoundedInputStream;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cluster.ConflictOverwriteChecker;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.sync.*;
import com.fasterxml.clustermate.service.util.StoreUtil;

public class RemoteClusterHandler<K extends EntryKey, E extends StoredEntry<K>>
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /*
    ///////////////////////////////////////////////////////////////////////
    // NOTE: some of these constants could be externalized in configs,
    //  but probably not all. Or maybe none. So do it once need arises.
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * And let's wait for 30 seconds after failed contact attempt to remote cluster
     */
    private final static long MSECS_TO_WAIT_AFTER_FAILED_STATUS = 30 * 1000L;

    /**
     * Also, if we fail to contact any of the peers, wait for 30 seconds as well.
     */
    private final static long MSECS_TO_WAIT_AFTER_NO_REMOTE_PEERS = 30 * 1000L;

    /**
     * And let's wait up to 10 seconds for remote cluster state messages.
     */
    private final static int MAX_WAIT_SECS_FOR_REMOTE_STATUS = 10;

//    private final static long SLEEP_FOR_SYNCLIST_ERRORS_MSECS = 10000L;

    private final static long SLEEP_FOR_SYNCPULL_ERRORS_MSECS = 3000L;

    /**
     * We'll do bit of sleep before starting remote-sync in general;
     * 10 seconds should be enough.
     */
    private final static long SLEEP_INITIAL_MSECS = 10 * 1000L;

    /**
     * When hitting end-of-input (list) for the first time, sleep for modest
     * amount of time (200 msec)
     */
    private final static long SLEEP_AFTER_FIRST_EOI = 200L;
    
    /**
     * Timeout for the first sync-list for each round should not be trivially
     * low, since it determines whether peer is considered to be live or not.
     * So let's use 10 seconds to make it unlikely that regular GC or bit of
     * overload would cause it.
     */
    private final static TimeSpan TIMEOUT_FOR_INITIAL_SYNCLIST_MSECS = new TimeSpan(10L, TimeUnit.SECONDS);

    private final static TimeSpan TIMEOUT_FOR_SYNCLIST = new TimeSpan(8L, TimeUnit.SECONDS);
    
    // // // Fetch-specific constants; duplication with ClusterPeerImpl
    
    /**
     * And barring errors we can process synclist/-pull for up to N seconds
     * from a single peer host. Start with 60 second runs.
     */
    private final static long MAX_TIME_FOR_SYNCPULL_MSECS = 60 * 1000L;

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
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Configuration
    ///////////////////////////////////////////////////////////////////////
     */

    protected final SharedServiceStuff _stuff;

    protected final NodeState _localState;

    protected final RemoteClusterStateFetcher _remoteFetcher;

    protected final Stores<K,E> _stores;

    protected final StoredEntryConverter<K,E,?> _entryConverter;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // State
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Flag used to request termination of the sync thread.
     */
    protected final AtomicBoolean _running = new AtomicBoolean(false);

    protected Thread _syncThread;

    /**
     * State of the remote cluster as we see it, with respect to local node
     * (and remote peers relevant to it).
     */
    protected final AtomicReference<RemoteCluster> _remoteCluster = new AtomicReference<RemoteCluster>();

    protected final SyncListAccessor _syncListAccessor;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, init
    ///////////////////////////////////////////////////////////////////////
     */

    public RemoteClusterHandler(SharedServiceStuff stuff,
            Stores<K,E> stores,
            Set<IpAndPort> bs, NodeState localNode)
    {
        _stuff = stuff;
        _localState = localNode;
        _stores = stores;
        _entryConverter = stuff.getEntryConverter();
        _remoteFetcher = new RemoteClusterStateFetcher(stuff, _running, bs, localNode);
        _syncListAccessor = new SyncListAccessor(stuff);
    }

    public RemoteCluster getRemoteCluster() {
        return _remoteCluster.get();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // StartAndStoppable
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public void start() throws Exception {
        Thread t;
        
        synchronized (this) {
            t = _syncThread;
            if (t == null) { // sanity check
                _running.set(true);
                _syncThread = t = new Thread(new Runnable() {
                    @Override
                    public void run() {

                        // First things first; lazy initialization, do initial DNS lookups to catch probs
                        if (!_remoteFetcher.init()) { // no valid IPs?
                            LOG.error("No valid end points found for {}: CAN NOT PROCEED WITH REMOTE SYNC",
                                    getName());
                        } else {
                            // and then looping if valid endpoints
                            syncLoop();
                        }
                    }
                });
                _syncThread.setDaemon(true);
                _syncThread.setName(getName());
                t.start();
            }
        }
    }

    @Override
    public void prepareForStop() throws Exception {
        _stop(false);
    }

    @Override
    public void stop() throws Exception {
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
                LOG.info("Stop requested (force? {}) for {} thread", forced, getName());
            }
        }
        if (t != null) {
//            t.notify();
            t.interrupt();
        }
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Update loop
    ///////////////////////////////////////////////////////////////////////
     */

    protected void syncLoop()
    {
//        public RemoteCluster fetch(int maxWaitSecs) throws IOException

        final long initialSleepMsecs = SLEEP_INITIAL_MSECS;
        LOG.info("Starting {} thread, will sleep for {} msec before operation",
                getName(), initialSleepMsecs);

        // Delay start just slightly since startup is slow time, don't want to pile
        // lots of background processing
        if (!_stuff.isRunningTests()) {
	        try {
	            Thread.sleep(initialSleepMsecs);
	        } catch (InterruptedException e) {
	            // will most likely just quit in a bit
	        }
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
                RemoteCluster remote = _remoteCluster();

                if (remote == null) {
                    continue;
                }
                long listedCount = _syncListPull(remote);
                _stuff.sleep(_sleepForMsecs(listedCount));
            } catch (InterruptedException e) {
                if (_running.get()) {
                    LOG.warn("syncLoop() interrupted without clearing '_running' flag; ignoring");
                }
                continue;
            } catch (Exception e) {
                LOG.warn("Uncaught processing exception during syncLoop(): ({}) {}",
                        e.getClass().getName(), e.getMessage());
                if (_running.get()) {
                    // Ignore failures during shutdown, so only increase here
                    try {
                        _stuff.getTimeMaster().sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
                    } catch (InterruptedException e2) { }
                }
            }
        }
    }
    
    protected long _syncListPull(RemoteCluster cluster) throws InterruptedException
    {
        /* Logic here is simple: try remote nodes, one at a time,
         * until you find one where first SYNCLIST call succeeds,
         * and do list/pull access until either running out of
         * entries, or consume maximum linear-scan time (like
         * 60 seconds).
         */

        for (RemoteClusterNode peer : cluster.getRemotePeers()) {
            // Lazy-load synced-up-to as needed
            ActiveNodeState pstate = peer.persisted();

            if (pstate == null) {
                try {
                    pstate = _stores.getRemoteNodeStore().findEntry(peer.getAddress());
                    if (pstate == null) {
                        pstate = new ActiveNodeState(_localState, peer.asNodeState(_localState),
                                _stuff.currentTimeMillis());
                    }
                    peer.setPersisted(pstate);
                } catch (Exception e) {
                    LOG.warn("Failed to load Remote Node State for {}; must skip node", peer);
                    continue;
                }
            }
            // Let's try initial call with relatively high timeout; if it succeeds,
            // we'll consider matching peer to be live and do actual sync
            try {
                SyncListResponse<?> fetchRemoteSyncList = _syncListAccessor
                        .fetchRemoteSyncList(_localState, peer.getAddress(),
                                pstate.getSyncedUpTo(), TIMEOUT_FOR_INITIAL_SYNCLIST_MSECS);
                // Returns null if call fails
                if (fetchRemoteSyncList != null) {
                    return _syncPull(peer, fetchRemoteSyncList);
                }
            } catch (InterruptedException e) { // presumably should bail out
                throw e;
            } catch (Exception e) {
                LOG.warn("Failure to fetch initial remote sync list from "+peer.getAddress()+", skipping peer: "
                        +" ("+e.getClass().getName()+") "+e.getMessage(), e);
            }
            // loop through
        }
        // If we get here, no luck
        LOG.warn("Failed to contact any of remote peers ({}) to do remote sync", cluster.getRemotePeers());
        _stuff.getTimeMaster().sleep(MSECS_TO_WAIT_AFTER_NO_REMOTE_PEERS);
        return 0L;
    }

    /**
     * @return Number of listed entries, if complete; positive, or, if timed out
     *    negative count
     */
    protected long _syncPull(RemoteClusterNode peer, SyncListResponse<?> listResponse)
        throws InterruptedException, IOException
    {
        final long processUntil = _stuff.currentTimeMillis() + MAX_TIME_FOR_SYNCPULL_MSECS;
        long total = 0L;
        ActiveNodeState savedState = null;
        int listCalls = 0;
        boolean seenEoi = false;

        ActiveNodeState pstate = peer.persisted();

        // Let's try to limit damage from infinite loops by second check
        // (ideally shouldn't need such ad hoc limit but...)

        while (_running.get() && ++listCalls < 500) {
            final int count = listResponse.size();
            total += count;
            if (count == 0) {
                break;
            }

            List<SyncListResponseEntry> newEntries = listResponse.entries;
            /*int tombstoneCount =*/ _handleTombstones(newEntries);
            // then filter out entries that we already have:
            _filterSeen(newEntries);
            if (!_running.get()) { // short-circuit during shutdown
                break;
            }
            // use real system time since it's measuring actual time taken (not virtual time for syncing)
            final long startTime = System.currentTimeMillis();
            if (!newEntries.isEmpty()) {
                int newCount = newEntries.size();
                AtomicInteger rounds = new AtomicInteger(0);
                /*long lastProcessed =*/ _fetchMissing(peer.getAddress(), newEntries, rounds);
                int fetched = newCount - newEntries.size();

                double secs = (_stuff.currentTimeMillis() - startTime) / 1000.0;
                String timeDesc = String.format("%.2f", secs);
                LOG.info("Fetched {}/{} missing entries ({} listed) from {} in {} seconds ({} rounds)",
                        new Object[] { fetched, newCount, count, peer.getAddress(), timeDesc, rounds.get()});
            }

            // One safety thing: let's persist synced-up-to after first round;
            // this to reduce likelihood of 'poison pills' from blocking sync pipeline
            long lastSeenTimestamp = listResponse.lastSeen();
            if (lastSeenTimestamp > 0L) {
                pstate = pstate.withSyncedUpTo(lastSeenTimestamp);
            } else {
                LOG.warn("Missing lastSeenTimestamp from sync-list to {}", peer.getAddress());
            }
            peer.setPersisted(pstate);
            if (_stuff.currentTimeMillis() >= processUntil) {
                // we use negative values to indicate time out... old-skool
                total = -total;
                break;
            }
            if (savedState == null) {
                savedState = pstate;
                _stores.getRemoteNodeStore().upsertEntry(peer.getAddress(), pstate);
            }
            // And then get more stuff...
            /* Except for one more thing: if we seem to be running out of entries,
             * let's not wait for trickles; inefficient to request stuff by ones and twos.
             * Instead, let's bail out for now, to induce bit more delay
             */
            if (listResponse.eoi) {
                // We'll take one end-of-input, wait a little; but bail on second
                if (seenEoi) {
                    break;
                }
                final long sleepMsec = SLEEP_AFTER_FIRST_EOI;
                
                // TODO: 04-Sep-2014, tatu: Left for now, for debugging, remote in future
                LOG.warn("Reached end of input to sync from {} (with {} listed, total {}), will sleep for {} msec",
                        peer.getAddress(), count, total, sleepMsec);
                seenEoi = true;
                _stuff.sleep(sleepMsec);
            }

            listResponse = _syncListAccessor
                    .fetchRemoteSyncList(_localState, peer.getAddress(),
                            pstate.getSyncedUpTo(), TIMEOUT_FOR_INITIAL_SYNCLIST_MSECS);
            lastSeenTimestamp = listResponse.lastSeen();
        }

        // Also make sure to update this timestamp
        if (savedState != pstate) {
            _stores.getRemoteNodeStore().upsertEntry(peer.getAddress(), pstate);
        }
        return total;
    }

    private long _fetchMissing(IpAndPort endpoint,
            List<SyncListResponseEntry> missingEntries, AtomicInteger rounds)
        throws InterruptedException
    {
        // initially create as big batches as possible
        int maxToFetch = missingEntries.size();
        int tries = 0;
        int fails = 0;
        long syncedUpTo = 0L;

        do {
            ++tries;
            final long startTime = System.currentTimeMillis();
            AtomicInteger payloadSize = new AtomicInteger(0);
            SyncPullRequest req = _buildRemoteSyncPullRequest(missingEntries, maxToFetch, payloadSize);
            final int expCount = req.size();

            if (expCount == 0) { // sanity check, shouldn't happen but...
                throw new IllegalStateException("Internal error: empty syncPullRequest list ("+missingEntries.size()+" missing entries)");
            }

            rounds.addAndGet(1);
            AtomicInteger status = new AtomicInteger(0);
            InputStream in = null;
            try {
                in = _syncListAccessor.readRemoteSyncPullResponse(req, TIMEOUT_FOR_SYNCLIST,
                        endpoint, status, payloadSize.get());
            } catch (java.net.ConnectException e) {
                ++fails;
                LOG.warn("Failed to connect Remote server "+endpoint+" to fetch missing entries", e);
                _stuff.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
            } catch (Exception e) {
                LOG.warn("Problem trying to make Remote syncPull call to fetch "+expCount+" entries: ("
                        +e.getClass().getName() + ") " + e.getMessage(), e);
                ++fails;
                _stuff.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
            }
            if (in == null) {
                LOG.warn("Problem trying to fetch {} entries, received status code of {}",
                        expCount, status.get());
                _stuff.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
                ++fails;
                continue;
            }

            Iterator<SyncListResponseEntry> it = missingEntries.iterator();
            int count = 0;
            int headerLength = 0;
            long payloadLength = 0;
            final PullProblems probs = new PullProblems();
            
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
                        ++probs.other;
                        LOG.warn("Server returned more than expected {} entries; ignoring rest!", expCount);
                        break;
                    }
                    // missing header? Unexpected, but not illegal
                    if (headerLength == 0) {
                        if (probs.missing++ == 0) {
                            LOG.warn("Missing entry {}/{} (from {}), id {}: expired? (will only report first)",
                                    new Object[] { count, expCount, endpoint, reqEntry.key});
                        }
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
                    _pullEntry(endpoint, reqEntry, header, in, probs);
                    syncedUpTo = reqEntry.insertionTime; 
                }
                if (count < expCount) {
                    // let's consider 0 entries to be an error, to prevent infinite loops
                    if (count == 0) {
                        LOG.warn("Server returned NO entries, when requested "+expCount);
                        ++fails;
                    }
                    LOG.warn("Server returned fewer entries than requested for sync pull: {} vs {} (in {} msecs)",
                            new Object[] { count, expCount, (System.currentTimeMillis() - startTime)});
                }
                if (probs.hasIssues()) {
                    LOG.warn("Problems with remote pull request from {}: {}", endpoint, probs);
                }
            } catch (Exception e) {
                LOG.warn("Problem trying to fetch syncPull entry {}/{} (header-length: {}, length: {}): ({}) {}",
                        new Object[] { count+1, expCount, headerLength, payloadLength, e.getClass().getName(), e.getMessage() } );
                _stuff.sleep(SLEEP_FOR_SYNCPULL_ERRORS_MSECS);
                ++fails;
            } finally {
                if (in != null) {
                    try { in.close(); } catch (Exception e) { // shouldn't really happen
                        LOG.warn("Failed to close HTTP stream: {}", e.getMessage());
                    }
                }
            }
        } while (fails < MAX_SYNC_FAILURES && !missingEntries.isEmpty() && tries < MAX_FETCH_TRIES);

        return syncedUpTo;
    }

    private SyncPullRequest _buildRemoteSyncPullRequest(List<SyncListResponseEntry> missingEntries,
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
     * Method that does the heavy lifting of pulling a single synchronized entry,
     * if and as necessary.
     */
    private void _pullEntry(IpAndPort endpoint,
            SyncListResponseEntry reqEntry, SyncPullEntry header,
            InputStream in, PullProblems probs)
        throws IOException
    {
        final StorableStore entryStore = _stores.getEntryStore();
        final StorableKey key = header.key;

        /* first things first: either read things in memory (for inline inclusion),
         * or pipe into a file.
         */
        long expSize = header.storageSize;
        // Sanity check: although rare, deletion could have occurred after we got
        // the initial sync list, so:
        if (header.isDeleted) {
            entryStore.softDelete(StoreOperationSource.SYNC, null, key, true, true);
            return;
        }
        StorableCreationResult result;
        StorableCreationMetadata stdMetadata = new StorableCreationMetadata(header.compression,
                header.checksum, header.checksumForCompressed);
        stdMetadata.uncompressedSize = header.size;
        stdMetadata.storageSize = header.storageSize;
        // 16-Apr-2014, tatu: Need to remember to set replica flag now
        stdMetadata.replicated = true;

        /* 25-Apr-2014, As per [#32], we need to compensate time-to-live settings so that
         *   it is not reset; rather, it stay as close to remaining TTL as possible.
         *   
         *   Note that this means that "maxTTLSecs" IS modified, and "minTTLSecs" NOT, since
         *   former is measured from creation and latter (if used) from last-access.
         */
        
        ByteContainer customMetadata = _entryConverter.createMetadata(_stuff.currentTimeMillis(),
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
            result = entryStore.upsertConditionally(StoreOperationSource.SYNC, null, key, data,
                    stdMetadata, customMetadata, true,
                    new ConflictOverwriteChecker(reqEntry.insertionTime));
        } else {
            /* 21-Sep-2012, tatu: Important -- we must ensure that store only reads
             *   bytes that belong to the entry payload. The easiest way is by adding
             *   a wrapper stream that ensures this...
             */
            BoundedInputStream bin = new BoundedInputStream(in, stdMetadata.storageSize, false);
            // 19-Sep-2013, tatu: May need to upsert, when resolving conflicts
            result = entryStore.upsertConditionally(StoreOperationSource.SYNC, null, key, bin,
                    stdMetadata, customMetadata, true,
                    new ConflictOverwriteChecker(reqEntry.insertionTime));

            if (result.succeeded() && !bin.isCompletelyRead()) { // error or warning?
                Storable entry = result.getNewEntry();
                long ssize = (entry == null) ? -1L : entry.getStorageLength();
                ++probs.other;
                LOG.warn("Problems with sync-pull for '{}': read {} bytes, should have read {} more; entry storageSize: {}",
                        new Object[] { header.key, bin.bytesRead(), bin.bytesLeft(), ssize });
            }
        }

        // should we care whether this was redundant or not?
        if (!result.succeeded()) {
            if (probs.redundant++ == 0) {
                if (result.getPreviousEntry() != null) {
                    // most likely ok: already had the entry
                    LOG.info("Redundant sync-pull for '{}' (from {}): entry already existed locally (will only report first)",
                            header.key, endpoint);
                } else {
                    // should this add to 'failCount'? For now, don't
                    LOG.warn("Failed sync-pull for '{}' (from {}): no old entry. Strange! (will only report first)",
                            header.key, endpoint);
                }
            }
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper methods copied from ClusterPeerImpl (may want to refactor)
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected int _handleTombstones(List<SyncListResponseEntry> entries)
        throws IOException, StoreException
    {
        final StorableStore entryStore = _stores.getEntryStore();
        int count = 0;
        Iterator<SyncListResponseEntry> it = entries.iterator();
        while (it.hasNext()) {
            SyncListResponseEntry entry = it.next();
            // Tombstone: if we have an entry, convert to a tombstone.
            /* 06-Jul-2012, tatu: But if we don't have one, should we create one?
             *   Could think of it either way; but for now, let's not waste time and space
             *   -- although, on remote side, there might be some benefit...
             */
            if (entry.deleted()) {
                ++count;
                it.remove();
                entryStore.softDelete(StoreOperationSource.SYNC, null, entry.key, true, true);
            }
        }
        return count;
    }
    protected void _filterSeen(List<SyncListResponseEntry> entries)
        throws IOException, StoreException
    {
        final StorableStore entryStore = _stores.getEntryStore();
        Iterator<SyncListResponseEntry> it = entries.iterator();
        while (it.hasNext()) {
            SyncListResponseEntry remoteEntry = it.next();
            // Although tombstones have been handled and removed,
            // need to pay attention here, since conflict resolution may be necessary.
            Storable localEntry = entryStore.findEntry(StoreOperationSource.SYNC, null, remoteEntry.key);
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
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected RemoteCluster _remoteCluster() throws IOException
    {
        // We still have valid setup?
        RemoteCluster rc = _remoteCluster.get();
        if (rc == null || !rc.isStillValid(_stuff.currentTimeMillis())) {
            rc = _remoteFetcher.fetch(MAX_WAIT_SECS_FOR_REMOTE_STATUS);
            if (_remoteCluster == null) {
                final long timeoutMsecs = MSECS_TO_WAIT_AFTER_FAILED_STATUS;
                LOG.warn("Failed to access remote cluster status information; will wait for {} msecs",
                        timeoutMsecs);
                try {
                    _stuff.getTimeMaster().sleep(timeoutMsecs);
                } catch (InterruptedException e) { }
            }
            _remoteCluster.set(rc);
        }
        return rc;
    }

    /**
     * Helper method called to figure out how long to sleep between "sync runs";
     * used to try to limit overhead of sync by enforcing bigger batches,
     * with delays in-between, over more frequent syncs done locally.
     */
    protected long _sleepForMsecs(long listedCount)
    {
        if (listedCount < 0L) { // time out: minimal sleep, 50 msec
            return 50L;
        }
        if (listedCount == 0L) { // no entries, maximal, 10 secs
            return 10000L;
        }
        if (listedCount < 100) { // if less than 100, let's give 5 seconds
            return 5000L;
        }
        if (listedCount < 500) { // <500, 2 seconds
            return 2000L;
        }
        if (listedCount < 1000) { // <1000, 1 second
            return 500L;
        }
        // otherwise, 500 milliseconds seems ok
        return 500L;
    }
    
    protected String getName() {
        return "RemoteClusterSync";
    }

    private static class PullProblems {
        public int redundant = 0;
        public int missing = 0;
        public int other = 0;

        public boolean hasIssues() {
            return (redundant > 0) || (missing > 0) || (other > 0);
        }

        @Override
        public String toString() {
            return new StringBuilder(60)
                .append(redundant).append(" redundant, ")
                .append(missing).append(" missing entries and ")
                .append(other).append(" other problems")
                .toString();
        }
    }
}
