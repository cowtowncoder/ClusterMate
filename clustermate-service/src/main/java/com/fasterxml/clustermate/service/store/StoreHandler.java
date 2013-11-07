package com.fasterxml.clustermate.service.store;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.skife.config.TimeSpan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableIterationCallback;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.service.*;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.http.StreamingEntityImpl;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.ExternalOperationMetrics;
import com.fasterxml.clustermate.service.msg.*;
import com.fasterxml.clustermate.service.util.SimpleLogThrottler;

/**
 * Class that handles coordination between front-end service layer (servlet,
 * jax-rs) and back-end storage layer.
 */
public abstract class StoreHandler<
    K extends EntryKey,
    E extends StoredEntry<K>,
    L extends ListItem
>
    extends HandlerBase
    implements StartAndStoppable
{
    /**
     * Let's not allow unlimited number of entries to traverse, no matter what.
     */
    private final static int MAX_MAX_ENTRIES = 500;

    /**
     * And for now strict time limit of 5 seconds
     */
    private final static long MAX_LIST_TIME_MSECS = 5000L;

    private final static ListLimits DEFAULT_LIST_LIMITS =
            ListLimits.defaultLimits()
                .withMaxEntries(MAX_MAX_ENTRIES)
                .withMaxMsecs(MAX_LIST_TIME_MSECS);

    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */

    protected final ServiceConfig _serviceConfig;
    
    /**
     * Whether server-side (auto-)compression is enabled or not.
     */
    protected final boolean _cfgCompressionEnabled;

    protected final int _cfgDefaultMinTTLSecs;
    protected final int _cfgDefaultMaxTTLSecs;

    /*
    /**********************************************************************
    /* Helper objects, general
    /**********************************************************************
     */

    protected final ClusterViewByServer _cluster;
    
    protected final Stores<K,E> _stores;

    protected final FileManager _fileManager;

    protected final TimeMaster _timeMaster;

    protected final EntryKeyConverter<K> _keyConverter;

    protected final StoredEntryConverter<K,E,L> _entryConverter;

    protected final ObjectMapper _objectMapper;
    
    protected final ObjectWriter _listJsonWriter;
    
    protected final ObjectWriter _listSmileWriter;

    // Do we want these output? Not for production, at least...
    // TODO: Externalize
    private final static boolean LOG_DUP_PUTS = false;
    
    // log dup puts at max rate of 1 per second
    protected final SimpleLogThrottler _dupPutsLogger = LOG_DUP_PUTS ?
            new SimpleLogThrottler(LOG, 1000) : null;
    
    /*
    /**********************************************************************
    /* Helper objects, deferred delete support
    /**********************************************************************
     */
    
    /**
     * Does store use deferred (queued) deletions?
     *
     * @since 0.9.8
     */
    protected final DeferredDeleter _deferredDeleter;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public StoreHandler(SharedServiceStuff stuff, Stores<K,E> stores,
            ClusterViewByServer cluster)
    {
        _cluster = cluster;
        _stores = stores;
        _fileManager = stuff.getFileManager();
        _timeMaster = stuff.getTimeMaster();
        _keyConverter = stuff.getKeyConverter();
        _cfgCompressionEnabled = stuff.getStoreConfig().compressionEnabled;

        _objectMapper = stuff.jsonMapper();
        _listJsonWriter = stuff.jsonWriter();
        _listSmileWriter = stuff.smileWriter();

        _serviceConfig = stuff.getServiceConfig();

        _entryConverter = stuff.getEntryConverter();
        // seconds used (over millis) to fit in 32-bit int when stored
        _cfgDefaultMinTTLSecs = (int) (_serviceConfig.cfgDefaultSinceAccessTTL.getMillis() / 1000L);
        _cfgDefaultMaxTTLSecs = (int) (_serviceConfig.cfgDefaultMaxTTL.getMillis() / 1000L);

        // Are we to do deferred deletions?
        _deferredDeleter = constructDeleter(stuff, stores);
    }

    /*
    /**********************************************************************
    /* Minimal service life-cycle to inform deferred deleter if necessary
    /**********************************************************************
     */
    
    @Override
    public void start() throws Exception
    {
        if (_deferredDeleter != null) {
            _deferredDeleter.start();
        }
    }

    @Override
    public void prepareForStop() throws Exception
    {
        if (_deferredDeleter != null) {
            _deferredDeleter.prepareForStop();
        }
    }

    @Override
    public void stop() throws Exception
    {
        if (_deferredDeleter != null) {
            _deferredDeleter.stop();
        }
    }

    /*
    /**********************************************************************
    /* Support for unit tests
    /**********************************************************************
     */

    public Stores<K,E> getStores() { return _stores; }

    /*
    /**********************************************************************
    /* Abstract and overridable methods for sub-classes
    /**********************************************************************
     */

    /**
     * Method called for PUT operations, to figure out which method of updating 
     * "last-access" information should be used, if any. When entries are successfully
     * PUT, information will be stored as part of entry metadata and does NOT
     * come from request.
     */
    protected abstract LastAccessUpdateMethod _findLastAccessUpdateMethod(ServiceRequest request, K key);
    
    /**
     * Method called to let implementation update last-accessed timestamp if necessary
     * when a piece of content is succesfully fetched with GET (exists and either is
     * not soft-deleted, or passes check for deletion)
     */
    protected void updateLastAccessedForGet(ServiceRequest request, ServiceResponse response,
            E entry, long accessTime) { }

    protected void updateLastAccessedForHead(ServiceRequest request, ServiceResponse response,
            E entry, long accessTime) { }

    /**
     * Method called to let implementation do whatever updates are needed when
     * specified entry has been (soft-)deleted. This may mean deletion of
     * the last-accessed entry.
     */
    protected void updateLastAccessedForDelete(ServiceRequest request, ServiceResponse response,
            K key, long accessTime) { }

    protected abstract DeferredDeleter constructDeleter(SharedServiceStuff stuff,
            Stores<K,?> stores);

    /*
    /**********************************************************************
    /* Additional metrics access
    /**********************************************************************
     */

    public void augmentOperationMetrics(AllOperationMetrics metrics)
    {
        ExternalOperationMetrics deleteMetrics = metrics.DELETE;
        if (deleteMetrics != null) { // just for sanity...
            _deferredDeleter.augmentMetrics(deleteMetrics);
        }
    }
    
    /*
    /**********************************************************************
    /* Content access (GET)
    /**********************************************************************
     */

    public ServiceResponse getEntry(ServiceRequest request, ServiceResponse response, K key)
        throws StoreException
    {
        return getEntry(request, response, key, null);
    }
    
    public ServiceResponse getEntry(ServiceRequest request, ServiceResponse response, K key,
            OperationDiagnostics diag)
        throws StoreException
    {
        String rangeStr = request.getHeader(ClusterMateConstants.HTTP_HEADER_RANGE_FOR_REQUEST);
        ByteRange range;
        try {
            range = request.findByteRange();
        } catch (IllegalArgumentException e) {
            return invalidRange(response, key, rangeStr, e.getMessage());
        }
        String acceptableEnc = request.getHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT_COMPRESSION);
        Storable rawEntry = null;
        final StorableStore entryStore = _stores.getEntryStore();

        try {
            rawEntry = entryStore.findEntry(StoreOperationSource.REQUEST,
                    diag, key.asStorableKey());
        } catch (IOException e) {
            return _storeError(response, key, e);
        }
        if (rawEntry == null) {
            return handleGetForMissing(request, response, key);
        }
        // second: did we get a tombstone?
        if (rawEntry.isDeleted()) {
            ServiceResponse resp = handleGetForDeleted(request, response, key, rawEntry);
            if (resp != null) {
                return resp;
            }
        }
        // [issue #7]: Conditional GET with Etag
        if (_notChanged(request, rawEntry)) {
            return response.notChanged();
        }

        final long accessTime = _timeMaster.currentTimeMillis();
        final E entry = _entryConverter.entryFromStorable(rawEntry);

        updateLastAccessedForGet(request, response, entry, accessTime);
        
        Compression comp = entry.getCompression();
        boolean skipCompression;

        // Range to resolve, now that we know full length?
        if (range != null) {
            range = range.resolveWithTotalLength(entry.getActualUncompressedLength());
            final long length = range.calculateLength();
            // any bytes matching? If not, it's a failure
            if (length <= 0L) {
                return response.badRange(new GetErrorResponse<K>(key, "Invalid 'Range' HTTP Header (\""+range+"\")"));
            }
            // note: can not skip decompress if we have to give range...
            skipCompression = false;
        } else {
            skipCompression = (comp != Compression.NONE) && comp.isAcceptable(acceptableEnc);
        }
        
        StreamingResponseContent output;

        if (entry.hasExternalData()) { // need to stream from File
            File f = entry.getRaw().getExternalFile(_fileManager);
            output = new FileBackedResponseContentImpl(diag, _timeMaster, entryStore,
                    accessTime, f, skipCompression ? null : comp, range, entry);
        } else { // inline
            ByteContainer inlined = entry.getRaw().getInlinedData();
            if (!skipCompression) {
                try {
                    inlined = Compressors.uncompress(inlined, comp, (int) entry.getRaw().getOriginalLength());
                } catch (IOException e) {
                    return internalGetError(response, e, key, "Failed to decompress inline data");
                }
            }
            output = new SimpleStreamingResponseContent(diag, _timeMaster, inlined, range, inlined.byteLength());
        }
        // #21: provide content length header
        long cl = output.getLength();
        if (cl >= 0L) {
            response = response.setContentLength(cl);
        }
        // one more thing; add header for range if necessary; also, response code differs
        if (range == null) {
            response = response.ok(output);
        } else {
            response = response.partialContent(output, range.asResponseHeader());
        }
        // Issue #6: Need to provide Etag, if content hash available
        int contentHash = rawEntry.getContentHash();
        if (contentHash != HashConstants.NO_CHECKSUM) {
            StringBuilder sb = new StringBuilder();
            sb.append('"');
            sb.append(contentHash);
            sb.append('"');
            response = response.addHeader(ClusterMateConstants.HTTP_HEADER_ETAG, sb.toString());
        }
        
        // also need to let client know we left compression in there:
        if (skipCompression) {
            response = response.setBodyCompression(comp.asContentEncoding());
        }
        return response;
    }

    protected boolean _notChanged(ServiceRequest request, Storable rawEntry)
    {
        // First: entry must have hash value to compare against
        int contentHash = rawEntry.getContentHash();
        if (contentHash != HashConstants.NO_CHECKSUM) {
            String rangeStr = request.getHeader(ClusterMateConstants.HTTP_HEADER_ETAG_NO_MATCH);
            if (rangeStr != null) {
                rangeStr = rangeStr.trim();
                if (rangeStr.length() > 2 && rangeStr.charAt(0) == '"') {
                    int ix = rangeStr.lastIndexOf('"');
                    if (ix > 0) {
                        rangeStr = rangeStr.substring(1, ix);
                        try {
                            // Parse as Long to allow for both signed and unsigned representations
                            // of 32-bit hash value
                            long l = Long.parseLong(rangeStr);
                            int i = (int) l;
                            return (i == contentHash);
                        } catch (IllegalArgumentException e) { }
                    }
                }
            }
        }
        return false;
    }
    
    /*
    /**********************************************************************
    /* Content access, metadata
    /**********************************************************************
     */

    public ServiceResponse getEntryStats(ServiceRequest request, ServiceResponse response, K key)
        throws StoreException
    {
        return getEntryStats(request, response, key, null);
    }
    
    // public for calling from unit tests
    public ServiceResponse getEntryStats(ServiceRequest request, ServiceResponse response, K key,
            OperationDiagnostics metadata)
        throws StoreException
    {
        // Do we need special handling for Range requests? (GET only?)
    	// Should this update last-accessed as well? (for now, won't)
        Storable rawEntry;
        try {
            rawEntry = _stores.getEntryStore().findEntry(StoreOperationSource.REQUEST,
                    metadata, key.asStorableKey());
        } catch (IOException e) {
            return _storeError(response, key, e);
        } 
        if (metadata != null) {
            metadata.setEntry(rawEntry);
        }
        if (rawEntry == null) {
            return response.notFound(new GetErrorResponse<K>(key, "No entry found for key '"+key+"'"));
        }
        // second: did we get a tombstone?
        if (rawEntry.isDeleted()) {
            return response.noContent();
        }

        final long accessTime = _timeMaster.currentTimeMillis();
        final E entry = _entryConverter.entryFromStorable(rawEntry);
        // should this be recorded in OpStats?
        updateLastAccessedForHead(request, response, entry, accessTime);
        
        // Other than this: let's only check out length of data there would be...
        final Compression comp = entry.getCompression();
        long size;
        
        // Would we return content as-is? (not compressed, or compressed using something
        // client accepts)
        String acceptableComp = request.getHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT_COMPRESSION);
        if (comp == Compression.NONE || comp.isAcceptable(acceptableComp)) {
            size = entry.getStorageLength();
        } else {
            size = entry.getActualUncompressedLength();
        }
        return response.ok().setContentLength(size);
    }

    /*
    /**********************************************************************
    /* Content insertion (PUT)
    /**********************************************************************
     */

    public ServiceResponse putEntry(ServiceRequest request, ServiceResponse response,
            K key, InputStream dataIn)
    {
        return putEntry(request, response, key, dataIn, null);
    }
    
    public ServiceResponse putEntry(ServiceRequest request, ServiceResponse response,
            K key, InputStream dataIn, OperationDiagnostics metadata)
    {
        final int checksum = _decodeInt(request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_CHECKSUM), 0);
        return putEntry(request, response, key, checksum, dataIn, null, null, metadata);
    }

    // Public due to unit tests
    public ServiceResponse putEntry(ServiceRequest request, ServiceResponse response,
            K key, int checksum,// 32-bit hash by client
            InputStream dataIn,
            TimeSpan minTTLSinceAccess, TimeSpan maxTTL,
            OperationDiagnostics stats)
    {
        final long  creationTime = _timeMaster.currentTimeMillis();

        // What compression, if any, is payload using?
        Compression inputCompression = Compression.forContentEncoding(request.getHeader(
                ClusterMateConstants.HTTP_HEADER_COMPRESSION));
        final StorableCreationMetadata stdMetadata = new StorableCreationMetadata(inputCompression,
                checksum, 0);
        if (inputCompression != null && inputCompression != Compression.NONE) {
            String valueStr = request.getHeader(ClusterMateConstants.CUSTOM_HTTP_HEADER_UNCOMPRESSED_LENGTH);
            long uncompLen = -1L;
            if (valueStr != null) {
                uncompLen = _decodeLong(valueStr, uncompLen);
            }
            if (uncompLen <= 0L) {
                final String prefix = (valueStr == null)? "Missing" : "Invalid";
                return response.badRequest
                        (PutResponse.badCompression(key, prefix+" value for header "
                                +ClusterMateConstants.CUSTOM_HTTP_HEADER_UNCOMPRESSED_LENGTH
                                +"; required for compression type of "+inputCompression));
            }
            stdMetadata.uncompressedSize = uncompLen;
        }

        // assumption here is that we may be passed hash code of orig content, but
        // not that of compressed (latter is easy to calculate on server anyway)
        ByteContainer customMetadata = constructPutMetadata(request, key, creationTime,
        		minTTLSinceAccess, maxTTL);
        StorableCreationResult result;

        try {
            /* This gets quite convoluted but that's how it goes: if undelete is
             * allowed, we must use different method:
             */
            if (_serviceConfig.cfgAllowUndelete) {
                result = _stores.getEntryStore().upsertConditionally(StoreOperationSource.REQUEST, stats,
                        key.asStorableKey(),
                        dataIn, stdMetadata, customMetadata, true,
                        AllowUndeletingUpdates.instance);
            } else {
                result = _stores.getEntryStore().insert(StoreOperationSource.REQUEST, stats,
                        key.asStorableKey(), dataIn, stdMetadata, customMetadata);
            }
        } catch (StoreException.Input e) { // something client did wrong
            switch (e.getProblem()) {
            case BAD_COMPRESSION:
                return response.badRequest
                        (PutResponse.badCompression(key, "Bad Compression information passed: "+e.getMessage()));
            case BAD_CHECKSUM:
                return response.badRequest
                        (PutResponse.badArg(key, "Bad Checksum information passed: "+e.getMessage()));
            case BAD_LENGTH:
                return response.badRequest
                        (PutResponse.badArg(key, "Bad Length information passed: "+e.getMessage()));
            }
            return internalPutError(response, key,
                    e, "Failed to PUT an entry: "+e.getMessage());
        } catch (IOException e) {
            return internalPutError(response, key,
            		e, "Failed to PUT an entry: "+e.getMessage());
        }

        // And then check whether it was a dup put; and if so, that checksums match
        Storable prev = result.getPreviousEntry();
        if (prev != null) {
            if (stats != null) {
                stats.setEntry(result.getNewEntry());
            }
            _logDuplicatePut(key);
            // first: will not allow "recreating" a soft-deleted entry
            if (prev.isDeleted()) {
                if (!_serviceConfig.cfgAllowUndelete) {
                    String prob = "Failed PUT: trying to recreate deleted entry '"+key+"'";
                    return response.gone(PutResponse.error(key, prev, prob));
                }
                // otherwise... we are ok, iff checksums match
            }
            // second: verify that checksums match:
            String prob = _verifyChecksums(prev, stdMetadata);
            if (prob != null) {
                return response.conflict(PutResponse.error(key, prev, "Failed PUT: trying to "
                        +(prev.isDeleted() ? "undelete" : "overwrite")
                        +" entry '"+key+"' but "+prob));
            }
        } else if (stats != null) {
            stats.setEntry(result.getNewEntry());
        }
        return response.ok(PutResponse.ok(key, result.getNewEntry()));
    }
    
    private String _verifyChecksums(Storable oldEntry, StorableCreationMetadata newEntry)
    {
        if (oldEntry.getContentHash() != newEntry.contentHash) { 
            return "checksums differ; old had 0x"+Integer.toHexString(oldEntry.getContentHash())+", new 0x"+Integer.toHexString(newEntry.contentHash);
        }
        if (oldEntry.getCompressedHash() != newEntry.compressedContentHash) {
            return "checksumForCompressed differ; old had 0x"+Integer.toHexString(oldEntry.getCompressedHash())
                    +", new 0x"+Integer.toHexString(newEntry.compressedContentHash);
        }
        Compression oldC = oldEntry.getCompression();
        Compression newC = newEntry.compression;
        if (newC == null) {
            newC = Compression.NONE;
        }
        if (oldC != newC) {
            return "entity compression differs; old had "+oldC+" new "+newC;
        }
        return null;
    }
    
    /*
    /**********************************************************************
    /* Content deletion
    /**********************************************************************
     */

    public ServiceResponse removeEntry(ServiceRequest request, ServiceResponse response, K key)
        throws IOException, StoreException
    {
        return removeEntry(request, response, key, null);
    }

    public ServiceResponse removeEntry(ServiceRequest request, ServiceResponse response, K key,
            OperationDiagnostics metadata)
        throws IOException, StoreException
    {
        // this may or may not result in deferred:
        long startTime = _timeMaster.currentTimeMillis();
        DeletionResult result;

        try {
            result = _deferredDeleter.addDeferredDeletion(key.asStorableKey(), startTime);
        } catch (Exception e) {
            LOG.error("Problem during DELETE scheduling: {}", e);
            return response.internalError("Failure due to: "+e);
        }

        switch (result.getStatus()) {
        case COMPLETED:
            response = response.ok(new DeleteResponse<K>(key));
            break;
        case DEFERRED:
            response = response.accepted(new DeleteResponse<K>(key));
            break;
        case QUEUE_FULL:
            return response.internalServerError();
        case TIMED_OUT:
            return response.serverOverload();
        case FAILED:
            {
                Throwable t = result.getRootCause();
                if (t instanceof StoreException) {
                    return _storeError(response, key, (StoreException) t);
                }
                return response.internalError("Failure due to: "+t);
            }
        default:
            LOG.error("Unrecognized status: "+result.getStatus());
            return response.internalServerError();
        }

        // If we got this far, can queue last-access deletion as well

        /* Even without match, we can claim it is ok... should we?
         * From idempotency perspective, result is that there is no such
         * entry; so let's allow that and just report ok.
         */
        // and finally, possibly remove matching last-accessed entry
        final long deleteTime = _timeMaster.currentTimeMillis();
        updateLastAccessedForDelete(request, response, key, deleteTime);
        
        return response;
    }

    /*
    /**********************************************************************
    /* Listing entries
    /**********************************************************************
     */
    
    /**
     * End point clients use to list entries with given name prefix, 
     * 
     * @param prefix Path prefix to use for filtering out entries not to list
     * @param stats Diagnostic information to update, if any
     * 
     * @return Modified response object
     */
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT listEntries(ServiceRequest request, OUT response,
            final K prefix, OperationDiagnostics stats)
        throws StoreException
    {
        // simple validation first
        if (prefix == null) {
            return (OUT) badRequest(response, "Missing path parameter for 'listEntries'");
        }
        String typeStr = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_TYPE);
        ListItemType listType = ListItemType.find(typeStr);
        if (listType == null) {
            if (typeStr == null || typeStr.isEmpty()) {
                return (OUT) badRequest(response, "Missing query parameter '"
                        +ClusterMateConstants.QUERY_PARAM_TYPE+"'");
            }
            return (OUT) badRequest(response, "Invalid query parameter '"
                    +ClusterMateConstants.QUERY_PARAM_TYPE+"', value '"+typeStr+"'");
        }
        
        /* First a sanity check: prefix should map to our active or passive range.
         * If not, we should not have any data to list; so let's (for now?) fail request:
         */
        int rawHash = _keyConverter.routingHashFor(prefix);
        // note: _cluster is null for testing, not for regular operation
        if ((_cluster != null) && !_cluster.getLocalState().inAnyRange(rawHash)) {
            return (OUT) badRequest(response, "Invalid prefix: not in key range (%s) of node",
                    _cluster.getLocalState().totalRange());
        }

        // Then extract 'lastSeen', if it's passed:
        StorableKey lastSeen = null;
        String b64str = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_LAST_SEEN);
        if (b64str != null && b64str.length() > 0) {
            try {
                // Jackson can do base64 decoding, and this is the easiest way
                byte[] lastSeenRaw = _objectMapper.convertValue(b64str, byte[].class);
                lastSeen = new StorableKey(lastSeenRaw);
            } catch (Exception e) {
                return (OUT) badRequest(response, "Invalid '"+ClusterMateConstants.QUERY_PARAM_LAST_SEEN
                        +"' value; not valid base64 encoded byte sequence");
            }
        }
        // Otherwise can start listing
        boolean useSmile = _acceptSmileContentType(request);
        final StorableKey rawPrefix = prefix.asStorableKey();
        ListLimits limits = DEFAULT_LIST_LIMITS;

        String maxStr = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_MAX_ENTRIES);
        if (maxStr != null) {
            limits = limits.withMaxEntries(_decodeInt(maxStr, limits.getMaxEntries()));
        }

        // !!! TODO: allow listing of tombstones?
        
        ListResponse<?> listResponse = null;
        
        switch (listType) {
        case ids:
            {
                List<StorableKey> ids = _listIds(stats, rawPrefix, lastSeen, limits);
                listResponse = new ListResponse.IdListResponse(ids, _last(ids));
            }
            break;
        case names:
            {
                List<StorableKey> ids = _listIds(stats, rawPrefix, lastSeen, limits);
                ArrayList<String> names = new ArrayList<String>(ids.size());
                for (StorableKey id : ids) {
                    names.add(_keyConverter.rawToString(id));
                }
                listResponse = new ListResponse.NameListResponse(names, _last(ids));
            }
            break;
        case minimalEntries:
        case fullEntries:
            {
                List<ListItem> items = _listItems(stats, listType, rawPrefix, lastSeen, limits);
                ListItem lastItem = _last(items);
                if (listType == ListItemType.minimalEntries) {
                    listResponse = new ListResponse.MinimalItemListResponse(items,
                            (lastItem == null) ? null : lastItem.getKey());
                } else {
                    listResponse = new ListResponse<ListItem>(items,
                            (lastItem == null) ? null : lastItem.getKey());
                }
            }
            break;
        default:
            throw new IllegalStateException();
        }

        if (stats != null) {
            stats.setItemCount(listResponse.size());
        }
        
        final ObjectWriter w = useSmile ? _listSmileWriter : _listJsonWriter;
        final String contentType = useSmile ? ContentType.SMILE.toString()
                : ContentType.JSON.toString();
        return (OUT) response.ok(contentType, new StreamingEntityImpl(w, listResponse));
    }

    protected final static <T> T _last(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        return list.get(list.size() - 1);
    }
    
    protected List<ListItem> _listItems(OperationDiagnostics diag,
            ListItemType itemType, StorableKey prefix,
            StorableKey lastSeen, ListLimits limits)
        throws StoreException
    {
        // we could check if all entries were iterated (with result code); for now we won't
        ListItemsCallback cb = (itemType == ListItemType.fullEntries)
                ? new FullListItemsCallback(_timeMaster, _entryConverter, prefix, limits)
                : new ListItemsCallback(_timeMaster, _entryConverter, prefix, limits)
                ;
        /* Two cases; either starting without last seen -- in which case we should include
         * the first entry -- or with last-seen, in which case it is to be skipped.
         */
        if (lastSeen == null) {
            // we could check if all entries were iterated (with result code); for now we won't
            /*IterationResult r =*/ _stores.getEntryStore().iterateEntriesByKey(StoreOperationSource.REQUEST, diag,
                    prefix, cb);
        } else {
            // we could check if all entries were iterated (with result code); for now we won't
            /*IterationResult r =*/ _stores.getEntryStore().iterateEntriesAfterKey(StoreOperationSource.REQUEST, diag,
                    lastSeen, cb);
        }
        return cb.getResult();
    }

    protected List<StorableKey> _listIds(final OperationDiagnostics diag,
            final StorableKey prefix, StorableKey lastSeen,
            final ListLimits limits)
        throws StoreException
    {
        final long maxTime = _timeMaster.currentTimeMillis() + limits.getMaxMsecs();
        final int maxEntries = limits.getMaxEntries();
        final ArrayList<StorableKey> result = new ArrayList<StorableKey>(100);
        final boolean includeTombstones = limits.getIncludeTombstones();

        StorableIterationCallback cb = new StorableIterationCallback() {
            public int counter; // to avoid checking systime too often

            @Override
            public IterationAction verifyKey(StorableKey key) {
                if (!key.hasPrefix(prefix)) {
                    return IterationAction.TERMINATE_ITERATION;
                }
                // Can add right away, if it's ok to include tombstones
                if (includeTombstones) {
                    return _addEntry(key);
                }
                // otherwise need to peek in entry itself
                return IterationAction.PROCESS_ENTRY;
            }

            private final IterationAction _addEntry(StorableKey key)
            {
                result.add(key);
                if (result.size() >= maxEntries) {
                    return IterationAction.TERMINATE_ITERATION;
                }
                if (((++counter & 15) == 0) // check for every 16 items
                    && _timeMaster.currentTimeMillis() >= maxTime) {
                        return IterationAction.TERMINATE_ITERATION;
                }
                // no need for entry; key has all the data
                return IterationAction.SKIP_ENTRY;
            }

            @Override
            public IterationAction processEntry(Storable entry) throws StoreException {
                if (includeTombstones || entry.isDeleted()) {
                    return IterationAction.SKIP_ENTRY;
                }
                return _addEntry(entry.getKey());
            }
        };
        /* Two cases; either starting without last seen -- in which case we should include
         * the first entry -- or with last-seen, in which case it is to be skipped.
         */
        if (lastSeen == null) {
            // we could check if all entries were iterated (with result code); for now we won't
            /*IterationResult r =*/ _stores.getEntryStore().iterateEntriesByKey(StoreOperationSource.REQUEST, diag,
                    prefix, cb);
        } else {
            // we could check if all entries were iterated (with result code); for now we won't
            /*IterationResult r =*/ _stores.getEntryStore().iterateEntriesAfterKey(StoreOperationSource.REQUEST, diag,
                    lastSeen, cb);
        }
        return result;
    }

    /*
    /**********************************************************************
    /* Customizable handling for query parameter access, defaulting
    /**********************************************************************
     */

    protected ByteContainer constructPutMetadata(ServiceRequest request, K key, long creationTime,
            TimeSpan minTTLSinceAccess, TimeSpan maxTTL)
    {
        if (minTTLSinceAccess == null) {
            minTTLSinceAccess = findMinTTLParameter(request, key);
        }
        if (maxTTL == null) {
            maxTTL = findMaxTTLParameter(request, key);
        }
        LastAccessUpdateMethod lastAcc = _findLastAccessUpdateMethod(request, key);
        int minTTLSecs = (minTTLSinceAccess == null) ? findMinTTLDefaultSecs(request, key)
                : (int) (minTTLSinceAccess.getMillis() / 1000);
        int maxTTLSecs = (maxTTL == null) ? findMaxTTLDefaultSecs(request, key)
                : (int) (maxTTL.getMillis() / 1000);

        return _entryConverter.createMetadata(creationTime,
                ((lastAcc == null) ? 0 : lastAcc.asByte()),
                minTTLSecs, maxTTLSecs);
    }
    
    /**
     * Overridable helper method used for figuring out request parameter used to
     * pass "minimum time-to-live since last access" (or, if no access tracked,
     * since creation).
     */
    protected TimeSpan findMinTTLParameter(ServiceRequest request, K key)
    {
        String paramKey = ClusterMateConstants.QUERY_PARAM_MIN_SINCE_ACCESS_TTL;
        return _timeSpanFrom(paramKey, request.getQueryParameter(paramKey));
    }

    /**
     * Overridable helper method used for figuring out request parameter used to
     * pass "maximum time-to-live since creation".
     */
    protected TimeSpan findMaxTTLParameter(ServiceRequest request, K key)
    {
        String paramKey = ClusterMateConstants.QUERY_PARAM_MAX_TTL;
        final String paramValue = request.getQueryParameter(paramKey);
        return _timeSpanFrom(paramKey, paramValue);
    }
    
    protected TimeSpan _timeSpanFrom(String key, String value)
    {
        if (_isEmpty(value)) {
            return null;
        }
        // Let's use bit of heuristics; pure number == seconds; otherwise, TimeSpan
        char c = value.charAt(value.length() - 1);
        try {
            if (c <= '9' && c >= '0') {
                return new TimeSpan(Integer.parseInt(value), TimeUnit.SECONDS);
            }
            return new TimeSpan(value);
        } catch (Exception e) {
    		    throw new IllegalArgumentException("Invalid value for '"+key+"': \""+value
    		            +"\": needs to be either number (seconds), or valid TimeSpan expression (like \"7d\")");
        }
    }

    protected int findMinTTLDefaultSecs(ServiceRequest request, K key) {
        return _cfgDefaultMinTTLSecs;
    }

    protected int findMaxTTLDefaultSecs(ServiceRequest request, K key) {
        return _cfgDefaultMaxTTLSecs;
    }
    
    /*
    /**********************************************************************
    /* Customizable handling for deleted and missing entries
    /**********************************************************************
     */
    
    /**
     * Method called to determine what to do when no entry was found for GET
     * with specified key.
     * Method must return a non-null response to return to sender.
     */
    protected ServiceResponse handleGetForDeleted(ServiceRequest request, ServiceResponse response,
            K key, Storable contents)
    {
        if (_serviceConfig.cfgReportDeletedAsEmpty) {
            return response.noContent();
        }
        return response.notFound(new GetErrorResponse<K>(key, "No entry found for key '"+key+"'"));
    }

    /**
     * Method called to determine what to do when a (soft-)deleted entry is found with GET.
     * Choices include sending a specific response (404 or 204, for example; or returning
     * null to indicate "handle normally".
     */
    protected ServiceResponse handleGetForMissing(ServiceRequest request, ServiceResponse response,
            K key)
    {
        return response.notFound(new GetErrorResponse<K>(key, "No entry found for key '"+key+"'"));
    }
    
    /*
    /**********************************************************************
    /* Error reporting
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    protected <OUT extends ServiceResponse> OUT _badRequest(ServiceResponse response, String msg) {
        return (OUT) response.badRequest(msg).setContentTypeText();
    }

    @SuppressWarnings("unchecked")
    protected  <OUT extends ServiceResponse> OUT  _storeError(ServiceResponse response, K key,
            IOException e) {
        String msg;
        if (key == null) {
            msg = "StoreException (key "+key+"): "+e.getMessage();
        } else {
            msg = "StoreException: "+e.getMessage();
        }
        
        // 18-Mar-2013, tatu: StoreExceptions are special enough (unless proven otherwise)
        //  such that we do want to log details -- to be tuned as necessary
        LOG.error(msg, e);
        
        return (OUT) response.serviceTimeout(msg).setContentTypeText();
    }

    private ServiceResponse invalidRange(ServiceResponse response,
            K key, String value, String errorMsg)
    {
        return response.badRequest
                (PutResponse.badArg(key, "Invalid 'Range' HTTP Header (\""+value+"\"), problem: "+errorMsg));
    }

    private ServiceResponse internalGetError(ServiceResponse response,
            Exception e, K key, String msg)
    {
        msg = "Failed GET, key '"+key+"': "+msg;
        if (e != null) {
            msg += " (error message: "+e.getMessage()+")";
            LOG.error("Internal error for GET request: "+msg, e);
        }
        return response.internalError(new GetErrorResponse<K>(key, msg));
    }

    private ServiceResponse internalPutError(ServiceResponse response,
            K key, Throwable e, String msg)
    {
        if (e != null) {
            e = _peel(e);
            msg = msg + ": "+e.getMessage();
            LOG.error("Internal error for PUT request: "+msg, e);
        }
        return response.internalError(PutResponse.error(key, msg));
    }

    /*
    /**********************************************************************
    /* Internal methods, diagnostics
    /**********************************************************************
     */

    protected void _logDuplicatePut(K key)
    {
        if (_dupPutsLogger != null) {
            _dupPutsLogger.logWarn("Duplicate PUT for key '{}'; success, same checksum", key);
        }
    }
    
    /*
    /**********************************************************************
    /* Other helper methods
    /**********************************************************************
     */

    private boolean _isEmpty(String value)
    {
        return (value == null || value.length() == 0);
    }
    
    /**
     * Crappy little parse function that will try to avoid exception if
     * possible (exceptions are exceptionally costly to construct),
     * defer to JDK standard parsing if thing looks ok
     */
    private final int _decodeInt(String input, int defaultValue)
    {
        if (input == null || input.length() == 0) {
            return defaultValue;
        }
        if ("0".equals(input)) {
            return 0;
        }
        final int len = input.length();
        int i = 0;
        if (input.charAt(0) == '-') {
            if (len > 1) {
                ++i;
            }
        }
        for (; i < len; ++i) {
            char c = input.charAt(i);
            if (c > '9' || c < '0') {
                // invalid... error or default?
                return defaultValue;
            }
        }
        // let's allow both positive (unsigned 32 int) and negative (signed); to do that
        // need to parse as Long, cast down.
        try {
            return (int) Long.parseLong(input);
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }
    
    private final long _decodeLong(String input, long defaultValue)
    {
        if (input == null || input.length() == 0) {
            return defaultValue;
        }
        if ("0".equals(input)) {
            return 0L;
        }
        final int len = input.length();
        int i = 0;
        if (input.charAt(0) == '-') {
            if (len > 1) {
                ++i;
            }
        }
        for (; i < len; ++i) {
            char c = input.charAt(i);
            if (c > '9' || c < '0') {
                // invalid... error or default?
                return defaultValue;
            }
        }
        // let's allow both positive (unsigned 32 int) and negative (signed); to do that
        // need to parse as Long, cast down.
        try {
            return Long.parseLong(input);
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }

    protected static Throwable _peel(Throwable t) {
        while (t.getCause() != null) {
                t = t.getCause();
        }
        return t;
    }

    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */

    protected static class FullListItemsCallback extends ListItemsCallback
    {
        public FullListItemsCallback(TimeMaster timeMaster, StoredEntryConverter<?,?,?> entryConverter,
                StorableKey prefix, ListLimits limits) {
            super(timeMaster, entryConverter, prefix, limits);
        }

        @Override
        protected ListItem toListItem(Storable entry) {
            return _entryConverter.fullListItemFromStorable(entry);
        }
    }
    
    protected static class ListItemsCallback extends StorableIterationCallback
    {
        protected final TimeMaster _timeMaster;
        protected final StoredEntryConverter<?,?,?> _entryConverter;

        protected final StorableKey prefix;
        protected final long maxTime;
        protected final int maxEntries;
        protected final boolean includeTombstones;

        protected final ArrayList<ListItem> result = new ArrayList<ListItem>(100);
        
        public int counter; // to avoid checking systime too often
        
        public ListItemsCallback(TimeMaster timeMaster, StoredEntryConverter<?,?,?> entryConverter,
                StorableKey prefix, ListLimits limits)
        {
            _timeMaster = timeMaster;
            _entryConverter = entryConverter;
            maxTime = _timeMaster.currentTimeMillis() + limits.getMaxMsecs();
            maxEntries = limits.getMaxEntries();
            includeTombstones = limits.getIncludeTombstones();
            this.prefix = prefix;
        }

        public List<ListItem> getResult() { return result; }
        
        @Override
        public IterationAction verifyKey(StorableKey key) {
            if (!key.hasPrefix(prefix)) {
                return IterationAction.TERMINATE_ITERATION;
            }
            if ((++counter & 15) == 0) { // check for every 16 items
                if (!result.isEmpty()) { // do NOT terminate after at least one entry included
                    if (_timeMaster.currentTimeMillis() >= maxTime) {
                        return IterationAction.TERMINATE_ITERATION;
                    }
                }
            }
            return IterationAction.PROCESS_ENTRY;
        }

        @Override
        public IterationAction processEntry(Storable entry) throws StoreException
        {
            if (includeTombstones || !entry.isDeleted()) {
                result.add(toListItem(entry));
                if (result.size() >= maxEntries) {
                    return IterationAction.TERMINATE_ITERATION;
                }
            }
            return IterationAction.PROCESS_ENTRY;
        }

        protected ListItem toListItem(Storable entry) {
            return _entryConverter.minimalListItemFromStorable(entry);
        }
    }
}
