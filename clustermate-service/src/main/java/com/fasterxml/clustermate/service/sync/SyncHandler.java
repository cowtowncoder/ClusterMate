package com.fasterxml.clustermate.service.sync;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.skife.config.TimeSpan;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.IterationResult;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;
import com.fasterxml.storemate.store.file.FileManager;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ClusterStatusMessage;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.api.NodeState;
import com.fasterxml.clustermate.service.HandlerBase;
import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.ServiceRequest;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerUpdatable;
import com.fasterxml.clustermate.service.http.StreamingEntityImpl;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Class that handles requests related to node-to-node synchronization
 * process.
 */
public class SyncHandler<K extends EntryKey, E extends StoredEntry<K>>
    extends HandlerBase
{
    /**
     * Since 'list sync' operation can potentially scan through sizable
     * chunk of the store, let's limit actual time allowed to be spent
     * on that. For now, 200 msecs seems reasonable.
     */
    private final static long MAX_LIST_PROC_TIME_IN_MSECS = 200L;

    /**
     * End marker we use to signal end of response
     */
    public final static int LENGTH_EOF = 0xFFFF;

    public final static int MAX_HEADER_LENGTH = 0x7FFF;

    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */
    
    protected final ClusterViewByServerUpdatable _cluster;

    protected final Stores<K,E> _stores;

    protected final EntryKeyConverter<K> _keyConverter;
    
    protected final StoredEntryConverter<K,E> _entryConverter;
    
    protected final FileManager _fileManager;

    protected final TimeMaster _timeMaster;

    // // Helpers for JSON/Smile:
    
    protected final ObjectWriter _syncListJsonWriter;
    
    protected final ObjectWriter _syncListSmileWriter;

    protected final ObjectWriter _syncPullSmileWriter;
    
    protected final ObjectWriter _errorJsonWriter;
    
    protected final ObjectReader _jsonSyncPullReader;

    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */
    
    protected final KeySpace _keyspace;

    /**
     * We will list entries up until N seconds from current time; this to reduce
     * likelihood that we see an entry we do not yet have, but are about to be
     * sent by client; that is, to reduce hysteresis caused by differing
     * arrival times of entries.
     */
    protected final TimeSpan _cfgSyncGracePeriod;
    
    /**
     * We will limit number of entries listed in couple of ways; count limitation
     * is mostly to limit response message size.
     * Note that this limit must sometimes be relaxed when there are blocks of
     * entries with identical last-modified timestamp.
     */
    protected final int _maxToListPerRequest;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public SyncHandler(SharedServiceStuff stuff, Stores<K,E> stores,
            ClusterViewByServer cluster)
    {
        this(stuff, stores, cluster,
                stuff.getServiceConfig().cfgMaxEntriesPerSyncList);
    }

    public SyncHandler(SharedServiceStuff stuff, Stores<K,E> stores,
            ClusterViewByServer cluster, int maxToListPerRequest)
    {
        _stores = stores;
        _cluster = (ClusterViewByServerUpdatable) cluster;
        _entryConverter = stuff.getEntryConverter();
        _fileManager = stuff.getFileManager();
        _timeMaster = stuff.getTimeMaster();
        _keyspace = cluster.getKeySpace();
        _keyConverter = stuff.getKeyConverter();
        _cfgSyncGracePeriod = stuff.getServiceConfig().cfgSyncGracePeriod;
        _syncListJsonWriter = stuff.jsonWriter().withDefaultPrettyPrinter();
        _syncListSmileWriter = stuff.smileWriter();
        _syncPullSmileWriter = stuff.smileWriter();
        _jsonSyncPullReader = stuff.jsonReader(SyncPullRequest.class);

        // error responses always as JSON:
        _errorJsonWriter = stuff.jsonWriter();
        _maxToListPerRequest = maxToListPerRequest;
    }

    /*
    /**********************************************************************
    /* Simple accessors
    /**********************************************************************
     */

    public ClusterViewByServer getCluster() {
        return _cluster;
    }
    
    /*
    /**********************************************************************
    /* API, file listing
    /**********************************************************************
     */
    
    /**
     * End point clients use to find out metadata for entries this node has,
     * starting with the given timestamp.
     */
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT listEntries(ServiceRequest request, OUT response,
            Long sinceL, OperationDiagnostics metadata)
        throws StoreException
    {
        // simple validation first
        if (sinceL == null) {
            return (OUT) badRequest(response, "Missing path parameter for 'list-since'");
        }
        Integer keyRangeStart = _findIntParam(request, ClusterMateConstants.HTTP_QUERY_PARAM_KEYRANGE_START);
        if (keyRangeStart == null) {
            return (OUT) missingArgument(response, ClusterMateConstants.HTTP_QUERY_PARAM_KEYRANGE_START);
        }
        Integer keyRangeLength = _findIntParam(request, ClusterMateConstants.HTTP_QUERY_PARAM_KEYRANGE_LENGTH);
        if (keyRangeLength == null) {
            return (OUT) missingArgument(response, ClusterMateConstants.HTTP_QUERY_PARAM_KEYRANGE_LENGTH);
        }
        long clusterHash = _findLongParam(request, ClusterMateConstants.HTTP_QUERY_CLUSTER_HASH);
        KeyRange range;
        try {
            range = _keyspace.range(keyRangeStart, keyRangeLength);
        } catch (Exception e) {
            return (OUT) badRequest(response, "Invalid key-range definition (start '%s', end '%s'): %s",
                    keyRangeStart, keyRangeLength, e.getMessage());
        }

        /* 20-Nov-2012, tatu: We can now piggyback auto-registration by sending minimal
         *   info about caller...
         */
        IpAndPort caller = getCallerQueryParam(request);
        if (caller != null) {
            _cluster.checkMembership(caller, 0L, range);
        }
        
        String acceptHeader = request.getHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT);
        // what do they request? If not known, serve JSON (assumed to be from browser)
        boolean useSmile = (acceptHeader != null)
                && acceptHeader.trim().indexOf(ClusterMateConstants.CONTENT_TYPE_SMILE) >= 0;
        long currentTime = _timeMaster.currentTimeMillis();

        final long upUntil = currentTime - _cfgSyncGracePeriod.getMillis();

        AtomicLong timestamp = new AtomicLong(0L);
        long since = (sinceL == null) ? 0L : sinceL.longValue();
        
        /* One more thing: let's sanity check that our key range overlaps request
         * range. If not, can avoid (possibly huge) database scan.
         */
        NodeState localState = _cluster.getLocalState();        
        List<E> entries;

        KeyRange localRange = localState.totalRange();
        if (localRange.overlapsWith(range)) {
            entries = _listEntries(range, since, upUntil, _maxToListPerRequest, timestamp);
        /*
System.err.println("Sync for "+_localState.getRangeActive()+" (slice of "+range+"); between "+sinceL+" and "+upUntil+", got "+entries.size()+"/"
+_stores.getEntryStore().getEntryCount()+" entries... (time: "+_timeMaster.currentTimeMillis()+")");
*/
        } else {
            LOG.warn("Sync list request by {} for range {}; does not overlap with local range of {}; skipping",
                    caller, range, localRange);
            entries = Collections.emptyList();
        }
        
        // one more twist; if no entries found, can sync up to 'upUntil' time...
        long lastSeen = timestamp.get();
        if (entries.isEmpty() && upUntil > lastSeen) {
            lastSeen = upUntil-1;
        }

        // One more thing: is cluster info requested?
        ClusterStatusMessage clusterStatus;
        long currentHash = _cluster.getHashOverState();
        clusterStatus = (clusterHash == 0L || clusterHash != currentHash) ?
                _cluster.asMessage() : null;
        
        final SyncListResponse<E> resp = new SyncListResponse<E>(entries, timestamp.get(),
                currentHash, clusterStatus);
        final ObjectWriter w = useSmile ? _syncListSmileWriter : _syncListJsonWriter;
        final String contentType = useSmile ? ClusterMateConstants.CONTENT_TYPE_SMILE : ClusterMateConstants.CONTENT_TYPE_JSON;
        
        return (OUT) response.ok(new StreamingEntityImpl(w, resp))
                .setContentType(contentType);
    }
    
    /*
    /**********************************************************************
    /* API, direct content download
    /* (no (un)compression etc)
    /**********************************************************************
     */

    /**
     * Access endpoint used by others nodes to 'pull' data for entries they are
     * missing.
     * Note that request payload must be JSON; could change to Smile in future if
     * need be.
     */
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT  pullEntries(ServiceRequest request, OUT response,
            InputStream in,
            OperationDiagnostics metadata) throws StoreException
    {
        SyncPullRequest requestEntity = null;
        try {
            requestEntity = _jsonSyncPullReader.readValue(in);
        } catch (Exception e) {
            return (OUT) badRequest(response, "JSON parsing error: %s", e.getMessage());
        }
        List<StorableKey> ids = requestEntity.entries;
        ArrayList<E> entries = new ArrayList<E>(ids.size());
        StorableStore store = _stores.getEntryStore();
        
        for (StorableKey key : ids) {
            Storable raw = store.findEntry(key);
            // note: this may give null as well; caller needs to check (converter passes null as-is)
            E entry = (E) _entryConverter.entryFromStorable(raw);
            entries.add(entry);
        }
        return (OUT) response.ok(new SyncPullResponse<E>(_fileManager, _syncPullSmileWriter, entries));
    }

    /*
    /**********************************************************************
    /* Helper methods, accessing entries
    /**********************************************************************
     */
    
    protected List<E> _listEntries(final KeyRange inRange,
            final long since, long upTo0, final int maxCount,
            final AtomicLong lastSeenTimestamp)
        throws StoreException
    {
        final StorableStore store = _stores.getEntryStore();
        /* 19-Sep-2012, tatu: Alas, it is difficult to make this work
         *   with virtual time, tests; so for now we will use actual
         *   real system time and not virtual time.
         *   May need to revisit in future.
         */
        final long realStartTime = _timeMaster.realSystemTimeMillis();
        // sanity check (better safe than sorry)
        if (upTo0 >= realStartTime) {
            throw new IllegalStateException("Argument 'upTo' too high ("+upTo0+"): can not exceed current time ("
                    +realStartTime);
        }
        /* one more thing: need to make sure we can skip entries that
         * have been updated concurrently (rare, but possible).
         */
        long oldestInFlight = store.getOldestInFlightTimestamp();
        if (oldestInFlight != 0L) {
            if (upTo0 > oldestInFlight) {
                // since it's rare, add INFO logging to see if ever encounter this
                LOG.info("Oldest in-flight ({}) higher than upTo ({}), use former as limit",
                        oldestInFlight, upTo0);
                upTo0 = oldestInFlight;
            }
        }
        final long upTo = upTo0;
        
        final long processUntil = realStartTime + MAX_LIST_PROC_TIME_IN_MSECS;
        
        final ArrayList<E> result = new ArrayList<E>(Math.min(100, maxCount));
        final AtomicInteger totalCounter = new AtomicInteger(0);

        IterationResult r = _stores.getEntryStore().iterateEntriesByModifiedTime(
            new StorableLastModIterationCallback() {
                int total = 0;
                K key = null;
                // to ensure List advances timestamp:
                boolean timestampHasAdvanced = false;

                /* We can do most efficient checks for timestamp range by
                 * verifying timestamp first, right off the index we are
                 * using...
                 */
                @Override
                public IterationAction verifyTimestamp(long timestamp) {
                    if (timestamp > upTo) {
                        /* 21-Sep-2012, tatu: Should we try to approximate latest
                         *  possible "lastSeen" timestamp here? As long as we avoid
                         *  in-flight-modifiable things, it would seem possible.
                         *  However, let's play this safe for now.
                         */
                        totalCounter.set(total);
                        return IterationAction.TERMINATE_ITERATION;
                    }
                    // First things first: we do want to know last seen entry that's "in range"
                    if (lastSeenTimestamp != null) {
                        lastSeenTimestamp.set(timestamp);
                    }
                    timestampHasAdvanced |= (timestamp > since);
                    return IterationAction.PROCESS_ENTRY;
                }
                
                // Most of filtering can actually be done with just keys...
                @Override public IterationAction verifyKey(StorableKey rawKey) {
                    // check time limits
                    if ((++total & 0x7F) == 0) {
                        if (timestampHasAdvanced &&
                                _timeMaster.realSystemTimeMillis() > processUntil) {
                            totalCounter.set(total);
                            return IterationAction.TERMINATE_ITERATION;
                        }
                    }
                    // and then verify that we are in range...
                    key = _keyConverter.rawToEntryKey(rawKey);
                    int hash = _keyConverter.routingHashFor(key);
                    if (inRange.contains(hash)) {
                        return IterationAction.PROCESS_ENTRY;
                    }
                    return IterationAction.SKIP_ENTRY;
                }

                @Override
                public IterationAction processEntry(Storable storable)
                {
                    E entry = _entryConverter.entryFromStorable(key, storable);
                    result.add(entry);
                    /* One limitation, however; we MUST advance timer beyond initial
                     * 'since' time. This may require including more than 'max' entries.
                     */
                    if (timestampHasAdvanced && result.size() >= maxCount) {
                        totalCounter.set(total);
                        return IterationAction.TERMINATE_ITERATION;
                    }
                    return IterationAction.PROCESS_ENTRY;
                }
            }, since);

        // "timeout" is indicated by termination at primary key:
        if (r == IterationResult.TERMINATED_FOR_KEY) {
            int totals = totalCounter.get();
            long msecs = _timeMaster.realSystemTimeMillis() - realStartTime;
            double secs = msecs / 1000.0;
            LOG.warn(String.format("Had to stop processing 'listEntries' after %.2f seconds; scanned through %d entries, collected %d entries",
                    secs, totals, result.size()));
        } else if (r == IterationResult.FULLY_ITERATED) {
            /* Oh. Also, if we got this far, we better update last-seen timestamp;
             * otherwise we'll be checking same last entry over and over again
             */
            if (lastSeenTimestamp != null) {
                lastSeenTimestamp.set(upTo);
            }
        }
        return result;
    }

    /*
    /**********************************************************************
    /* Helper methods, other
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT missingArgument(ServiceResponse response, String argId) {
        return (OUT) badRequest(response, "Missing query parameter '"+argId+"'");
    }

    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT invalidArgument(ServiceResponse response, String argId, String argValue)
    {
        if (argValue == null) {
            return (OUT) missingArgument(response, argId);
        }
        return (OUT) badRequest(response, "Invalid query parameter '"+argId+"': value '"+argValue+"'");
    }
    
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT badRequest(ServiceResponse response, String errorTemplate, Object... args) {
        return (OUT) badRequest(response, String.format(errorTemplate, args));
    }

    @SuppressWarnings("unchecked")
    private <OUT extends ServiceResponse> OUT badRequest(ServiceResponse response, String error) {
        return (OUT) response
                .badRequest(new SyncListResponse<E>(error))
                .setContentTypeJson();
    }
}
