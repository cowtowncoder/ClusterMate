package com.fasterxml.clustermate.service.sync;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.storemate.store.backend.IterationResult;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerUpdatable;
import com.fasterxml.clustermate.service.msg.StreamingEntityImpl;
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
     * on that. For now, 400 msecs seems reasonable.
     */
    private final static long MAX_LIST_PROC_TIME_IN_MSECS = 400L;

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

    protected final StoredEntryConverter<K,E,?> _entryConverter;

    protected final EntryKeyConverter<K> _keyConverter;
    
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

    /**
     * We will list entries up until N seconds from current time; this to reduce
     * likelihood that we see an entry we do not yet have, but are about to be
     * sent by client; that is, to reduce hysteresis caused by differing
     * arrival times of entries.
     */
    protected final long _cfgSyncGracePeriodMsecs;

    protected final long _cfgMaxTimeToLiveMsecs;

    protected final long _cfgMaxLongPollTimeMsecs;
    
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
        _keyConverter = stuff.getKeyConverter();
        _cfgSyncGracePeriodMsecs = stuff.getServiceConfig().cfgSyncGracePeriod.getMillis();
        _cfgMaxTimeToLiveMsecs = stuff.getServiceConfig().cfgMaxMaxTTL.getMillis();
        _cfgMaxLongPollTimeMsecs = stuff.getServiceConfig().cfgSyncMaxLongPollTime.getMillis();
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

    public long getSyncGracePeriodMsecs() {
        return _cfgSyncGracePeriodMsecs;
    }
    
    /*
    /**********************************************************************
    /* API, file listing
    /**********************************************************************
     */

    /**
     * End point local cluster clients use to find out metadata for entries this node has,
     * starting with the given timestamp.
     * One side-effect of requests serve this way is that calling node may be automatically
     * registered as part of this cluster.
     */
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT localListEntries(ServiceRequest request, OUT response,
            Long sinceL, OperationDiagnostics metadata)
        throws InterruptedException, StoreException
    {
        return (OUT) _listEntries(request, response, sinceL, metadata, true);
    }

    /**
     * End point remote cluster clients use to find out metadata for entries this node has,
     * starting with the given timestamp.
     * Unlike local calls, auto-registration does not occur.
     */
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT remoteListEntries(ServiceRequest request, OUT response,
            Long sinceL, OperationDiagnostics metadata)
        throws InterruptedException, StoreException
    {
        return (OUT) _listEntries(request, response, sinceL, metadata, false);
    }
    
    /**
     * @param isLocal Whether this call is from local cluster (end point) or not (remote)
     */
    protected ServiceResponse _listEntries(ServiceRequest request, ServiceResponse response,
            Long sinceL, OperationDiagnostics metadata,
            boolean isLocal)
        throws InterruptedException, StoreException
    {
        // simple validation first
        if (sinceL == null) {
            return badRequest(response, "Missing path parameter for 'list-since'");
        }
        Integer keyRangeStart = _findIntParam(request, ClusterMateConstants.QUERY_PARAM_KEYRANGE_START);
        if (keyRangeStart == null) {
            return missingArgument(response, ClusterMateConstants.QUERY_PARAM_KEYRANGE_START);
        }
        Integer keyRangeLength = _findIntParam(request, ClusterMateConstants.QUERY_PARAM_KEYRANGE_LENGTH);
        if (keyRangeLength == null) {
            return missingArgument(response, ClusterMateConstants.QUERY_PARAM_KEYRANGE_LENGTH);
        }
        long clusterHash = _findLongParam(request, ClusterMateConstants.QUERY_PARAM_CLUSTER_HASH);
        KeyRange range;
        try {
            range = _cluster.getKeySpace().range(keyRangeStart, keyRangeLength);
        } catch (Exception e) {
            return badRequest(response, "Invalid key-range definition (start '%s', end '%s'): %s",
                    keyRangeStart, keyRangeLength, e.getMessage());
        }

        /* 20-Nov-2012, tatu: We can now piggyback auto-registration by sending minimal
         *   info about caller...
         */
        // 29-May-2014, tatu: Important! Only when handling local requests
        IpAndPort caller = getCallerQueryParam(request);
        if (isLocal) {
            if (caller != null) {
                _cluster.checkMembership(caller, 0L, range);
            }
        }
        boolean useSmile = _acceptSmileContentType(request);
        final long currentTime = _timeMaster.currentTimeMillis();
        long since = (sinceL == null) ? 0L : sinceL.longValue();

        /* [Issue#8] Let's impose minimum timestamp to consider, to try to avoid
         * exposing potentially obsolete entries (ones that are due to cleanup
         * and will disappear anyway).
         */
        long minSince = currentTime - _cfgMaxTimeToLiveMsecs;
        if (minSince > since) {
            LOG.warn("Sync list 'since' argument of {} updated to {}, to use maximum TTL of {}",
                    since, minSince, _cfgMaxTimeToLiveMsecs);
            since = minSince;
        }
        
        /* One more thing: let's sanity check that our key range overlaps request
         * range. If not, can avoid (possibly huge) database scan.
         */
        NodeState localState = _cluster.getLocalState();
        final SyncListResponse<E> resp;
        KeyRange localRange = localState.totalRange();
        if (localRange.overlapsWith(range)) {
            try {
                resp = _listEntries(range, since, _maxToListPerRequest);
            } catch (StoreException e) {
                return _storeError(response, e);
            }
        } else {
            LOG.warn("Sync list request by {} for range {}; does not overlap with local range of {}; skipping",
                    (caller == null) ? "UNKNOWN" : caller, range, localRange);
            resp = SyncListResponse.emptyResponse();
        }
        if (metadata != null) {
            metadata = metadata.setItemCount(resp.size());
        }
        long currentHash = _cluster.getHashOverState();
        resp.setClusterHash(currentHash);
        ClusterStatusMessage clusterStatus = (clusterHash == 0L || clusterHash != currentHash) ?
                _cluster.asMessage() : null;
        resp.setClusterStatus(clusterStatus);                
        final ObjectWriter w = useSmile ? _syncListSmileWriter : _syncListJsonWriter;
        final String contentType = useSmile ? ContentType.SMILE.toString() : ContentType.JSON.toString();
        
        return response.ok(new StreamingEntityImpl(w, resp))
                .setContentType(contentType);
    }
    
    /*
    /**********************************************************************
    /* API, direct content download
    /* (no (un)compression etc)
    /**********************************************************************
     */

    /**
     * Access endpoint used by other local nodes to 'pull' data for entries they are
     * missing.
     * Note that request payload must be JSON; could change to Smile in future if
     * need be.
     */
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT localPullEntries(ServiceRequest request, OUT response,
            InputStream in,
            OperationDiagnostics metadata)
        throws IOException, StoreException
    {
        return (OUT) _pullEntries(request, response, in, metadata, true);
    }
    
    /**
     * Access endpoint used by nodes of remote clusters to 'pull' data for entries they are
     * missing.
     */
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT remotePullEntries(ServiceRequest request, OUT response,
            InputStream in,
            OperationDiagnostics metadata)
        throws IOException, StoreException
    {
        return (OUT) _pullEntries(request, response, in, metadata, false);
    }
    
    public ServiceResponse _pullEntries(ServiceRequest request, ServiceResponse response,
            InputStream in, OperationDiagnostics metadata,
            boolean isLocal)
        throws IOException, StoreException
    {
        SyncPullRequest requestEntity = null;
        try {
            requestEntity = _jsonSyncPullReader.readValue(in);
        } catch (Exception e) {
            return badRequest(response, "JSON parsing error: %s", e.getMessage());
        }
        // Bit of validation, as unknown props are allowed:
        if (requestEntity.hasUnknownProperties()) {
            LOG.warn("Unrecognized properties in SyncPullRequest: "+requestEntity.unknownProperties());
        }
        
        List<StorableKey> ids = requestEntity.entries;
        ArrayList<E> entries = new ArrayList<E>(ids.size());
        StorableStore store = _stores.getEntryStore();

        try {
            for (StorableKey key : ids) {
                // null -> let's not pass 'metadata' yet since we don't use timings?
                Storable raw = store.findEntry(StoreOperationSource.SYNC, null, key);
                // note: this may give null as well; caller needs to check (converter passes null as-is)
                E entry = (E) _entryConverter.entryFromStorable(raw);
                entries.add(entry);
            }
        } catch (StoreException e) {
            return _storeError(response, e);
        } finally {
            if (metadata != null) {
                metadata = metadata.setItemCount(entries.size());
            }
        } 
        return response.ok(new SyncPullResponse<E>(_fileManager, _timeMaster.currentTimeMillis(),
                _syncPullSmileWriter, entries));
    }

    /*
    /**********************************************************************
    /* Helper methods, accessing entries
    /**********************************************************************
     */
    
    protected SyncListResponse<E> _listEntries(final KeyRange inRange,
            final long since, final int maxCount)
        throws InterruptedException, StoreException
    {
        final StorableStore store = _stores.getEntryStore();
        final ArrayList<E> result = new ArrayList<E>(Math.min(100, maxCount));
        long lastSeenTimestamp = 0L;
        long clientWait = 0L; // we may instruct client to do bit of waiting before retry
        
        // let's only allow single wait; hence two rounds
        long upTo0 = 0;
        for (int round = 0; round < 2; ++round) {
            upTo0 = _timeMaster.currentTimeMillis() - _cfgSyncGracePeriodMsecs;

            /* 19-Sep-2012, tatu: Alas, it is difficult to make this work with virtual time,
             *   tests; so for now we will use actual real system time and not virtual time.
             *   May need to revisit in future.
             */
            final long realStartTime = _timeMaster.realSystemTimeMillis();
            if (upTo0 >= realStartTime) { // sanity check (better safe than sorry)
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
    
            LastModLister<K,E> cb = new LastModLister<K,E>(_timeMaster, _entryConverter, inRange,
                    since, upTo, processUntil, maxCount, result);
            IterationResult r = _stores.getEntryStore().iterateEntriesByModifiedTime(StoreOperationSource.REQUEST,
                    // null -> no need for diagnostics/timing info
                    null, since, cb);

            // "timeout" is indicated by termination at primary key:
            if (r == IterationResult.TERMINATED_FOR_KEY) {
                lastSeenTimestamp = cb.getLastSeenTimestamp();
                int totals = cb.getTotal();
                long msecs = _timeMaster.realSystemTimeMillis() - realStartTime;
                double secs = msecs / 1000.0;
                LOG.warn(String.format("Had to stop processing 'listEntries' after %.2f seconds; scanned through %d entries, collected %d entries",
                        secs, totals, result.size()));
                break;
            }

            // No waiting if there are results
            if (result.size() > 0) {
                if (r == IterationResult.FULLY_ITERATED) { // means we got through all data (nothing to see)
                    // Oh. Also, if we got this far, we better update last-seen timestamp;
                    // otherwise we'll be checking same last entry over and over again
                    lastSeenTimestamp = upTo;
                } else {
                    lastSeenTimestamp = cb.getLastSeenTimestamp();
                }
                break;
            }
            
            // Behavior for empty lists differs between first and second round.
            // During first round, we will try bit of server-side sleep (only)
            if (round == 0) {
                long delay;
                if (r == IterationResult.TERMINATED_FOR_TIMESTAMP) {
                    // and running out of valid data by terminating for timestamp; if so, we have seen
                    // a later timestamp, but one that's not out of sync grace period yet
                    long targetTime = (cb.getNextTimestamp() + _cfgSyncGracePeriodMsecs);
                    delay = targetTime - _timeMaster.currentTimeMillis();
                } else if (r == IterationResult.FULLY_ITERATED) { // means we got through all data (nothing to see)
                    lastSeenTimestamp = upTo;
                    delay = _cfgSyncGracePeriodMsecs;
                } else { // this then should be TERMINATED_FOR_ENTRY, which means entries were added, no need for delay
                    break;
                }
                if (delay <= 0L) { // sanity check, should not occur
                    LOG.warn("No SYNCs to list, but calculated delay is {}, which is invalid (result = {}); ignoring",
                            delay, r);
                } else {
//LOG.warn("Server long-poll wait: {} msecs", delay);
                    Thread.sleep(Math.min(_cfgMaxLongPollTimeMsecs, delay));
                }
            } else {
                // and during second round, inform client how long it should sleep
                if (r == IterationResult.TERMINATED_FOR_TIMESTAMP) {
                    long targetTime = (cb.getNextTimestamp() + _cfgSyncGracePeriodMsecs);
                    clientWait = targetTime - _timeMaster.currentTimeMillis();

                    /*
LOG.error(" TERMINATED_FOR_TIMESTAMP: next stamp={}, graceMsecs={}, target={}, current={}", cb.getNextTimestamp(), _cfgSyncGracePeriodMsecs,
targetTime, _timeMaster.currentTimeMillis());
*/

                } else if (r == IterationResult.FULLY_ITERATED) {
                    lastSeenTimestamp = upTo;
                    clientWait = _cfgSyncGracePeriodMsecs;
                } // otherwise should be TERMINATED_FOR_ENTRY, which means entries were added, no need for delay

//LOG.warn("Server setting clientWait at {} msecs", clientWait);

                if (clientWait < 0L) { // sanity check, should not occur
                    LOG.warn("No SYNCs to list (round {}), but calculated clientWait is {}, which is invalid (result = {}); ignoring",
                            round, clientWait, r);
                }
            }
        }
        SyncListResponse<E> resp = new SyncListResponse<E>(result);
        // one more twist; if no entries found, can sync up to 'upUntil' time...
        if (result.size() == 0 && upTo0 > lastSeenTimestamp) {
            lastSeenTimestamp = upTo0-1;
        }
        resp.setLastSeenTimestamp(lastSeenTimestamp);
        if (clientWait > 0L) {
            resp.setClientWait(clientWait);
        }

        return resp;
    }
    
    /*
    /**********************************************************************
    /* Helper methods, other
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    protected <OUT extends ServiceResponse> OUT _badRequest(ServiceResponse response, String msg) {
        return (OUT) response.badRequest(new SyncListResponse<E>(msg)).setContentTypeJson();
    }

    @SuppressWarnings("unchecked")
    protected  <OUT extends ServiceResponse> OUT  _storeError(ServiceResponse response, StoreException e) {
        String msg = "StoreException: "+e.getMessage();
        // 18-Mar-2013, tatu: StoreExceptions are special enough (unless proven otherwise)
        //  such that we do want to log details -- to be tuned as necessary
        LOG.error(msg, e);
        return (OUT) response.serviceTimeout(new SyncListResponse<E>(msg)).setContentTypeJson();
    }
}

