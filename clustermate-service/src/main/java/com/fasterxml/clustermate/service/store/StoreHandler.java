package com.fasterxml.clustermate.service.store;

import java.io.*;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.file.FileManager;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.ServiceRequest;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.msg.*;

/**
 * Class that handles coordination between front-end service layer (servlet,
 * jax-rs) and back-end storage layer.
 */
public abstract class StoreHandler<K extends EntryKey, E extends StoredEntry<K>>
{
    // Do we want these output? Not for production, at least...
    // TODO: Externalize
    private final static boolean LOG_DUP_PUTS = false;

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */

    protected final Stores<K,E> _stores;

    protected final FileManager _fileManager;

    protected final TimeMaster _timeMaster;

    protected final EntryKeyConverter<K> _keyConverter;

    protected final StoredEntryConverter<K, E> _entryConverter;

    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */

    /**
     * Whether server-side (auto-)compression is enabled or not.
     */
    protected final boolean _cfgCompressionEnabled;

    /**
     * Whether it is ok to PUT/POST entries to "undelete" formerly
     * DELETEd entries (assuming hashes still match)
     */
    protected final boolean _cfgAllowUndelete;

    /**
     * Whether to return 204 or 404 for tombstone entries (entries
     * recently deleted): 204 if true, 404 if false.
     */
    protected final boolean _cfgReportDeletedAsEmpty;

    protected final int _cfgDefaultMinTTLSecs;
    protected final int _cfgDefaultMaxTTLSecs;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public StoreHandler(SharedServiceStuff stuff, Stores<K,E> stores)
    {
        _stores = stores;
        _fileManager = stuff.getFileManager();
        _timeMaster = stuff.getTimeMaster();
        _keyConverter = stuff.getKeyConverter();

        _cfgCompressionEnabled = stuff.getStoreConfig().compressionEnabled;

        final ServiceConfig config = stuff.getServiceConfig();

        _cfgAllowUndelete = config.cfgAllowUndelete;
        _cfgReportDeletedAsEmpty = config.cfgReportDeletedAsEmpty;

        _entryConverter = stuff.getEntryConverter();
        _cfgDefaultMinTTLSecs = (int) config.cfgDefaultSinceAccessTTL.getMillis();
        _cfgDefaultMaxTTLSecs = (int) config.cfgDefaultMaxTTL.getMillis();
    }

    /*
    /**********************************************************************
    /* Support for unit tests
    /**********************************************************************
     */

    public Stores<K,E> getStores() { return _stores; }
    
    /*
    /**********************************************************************
    /* Content access (GET)
    /**********************************************************************
     */
    
    // public since tests need to call it
    public ServiceResponse getEntry(ServiceRequest request, ServiceResponse response, K key)
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
        Storable rawEntry = _stores.getEntryStore().findEntry(key.asStorableKey());
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
        
        StreamingResponseContentImpl output;
        if (entry.hasExternalData()) { // need to stream from File
            File f = entry.getRaw().getExternalFile(_fileManager);
            output = new StreamingResponseContentImpl(f, skipCompression ? null : comp, range);
        } else { // inline
            ByteContainer inlined = entry.getRaw().getInlinedData();
            if (!skipCompression) {
                try {
                    inlined = Compressors.uncompress(inlined, comp, (int) entry.getRaw().getOriginalLength());
                } catch (IOException e) {
                    return internalGetError(response, e, key, "Failed to decompress inline data");
                }
            }
            output = new StreamingResponseContentImpl(inlined, range);
        }
        // one more thing; add header for range if necessary; also, response code differs
        if (range == null) {
            response = response.ok(output);
        } else {
            response = response.partialContent(output, range.asResponseHeader());
        }
        // also need to let client know we left compression in there:
        if (skipCompression) {
            response = response.setBodyCompression(comp.asContentEncoding());
        }
        return response;
    }
    
    /*
    /**********************************************************************
    /* Content access, metadata
    /**********************************************************************
     */

    // public for calling from unit tests
    public ServiceResponse getEntryStats(ServiceRequest request,
            ServiceResponse response, K key)
    	throws StoreException
    {
        // Do we need special handling for Range requests? (GET only?)
    	// Should this update last-accessed as well? (for now, won't)
        Storable rawEntry = _stores.getEntryStore().findEntry(key.asStorableKey());
        if (rawEntry == null) {
            return response.notFound(new GetErrorResponse<K>(key, "No entry found for key '"+key+"'"));
        }
        // second: did we get a tombstone?
        if (rawEntry.isDeleted()) {
            return response.noContent();
        }

        final long accessTime = _timeMaster.currentTimeMillis();
        final E entry = _entryConverter.entryFromStorable(rawEntry);
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
        final int checksum = _decodeInt(request.getQueryParameter(ClusterMateConstants.HTTP_QUERY_PARAM_CHECKSUM), 0);
        String paramKey = null, paramValue = null;
        paramKey = ClusterMateConstants.HTTP_QUERY_PARAM_MIN_SINCE_ACCESS_TTL;
        paramValue = request.getQueryParameter(paramKey);
        TimeSpan minTTL = _isEmpty(paramValue) ? null : new TimeSpan(paramValue);
        paramKey = ClusterMateConstants.HTTP_QUERY_PARAM_MAX_TTL;
        paramValue = request.getQueryParameter(paramKey);
        TimeSpan maxTTL = _isEmpty(paramValue) ? null : new TimeSpan(paramValue);

        return putEntry(request, response, key, checksum, dataIn,
                minTTL, maxTTL);
    }   

    // Public due to unit tests
    public ServiceResponse putEntry(ServiceRequest request, ServiceResponse response,
            K key, int checksum,// 32-bit hash by client
            InputStream dataIn,
            TimeSpan minTTLSinceAccess, TimeSpan maxTTL)
    {
        final long  creationTime = _timeMaster.currentTimeMillis();
    	
        // first things first: ensure that request was correctly sent wrt routing
        Compression inputCompression = Compression.forContentEncoding(request.getHeader(
                ClusterMateConstants.HTTP_HEADER_COMPRESSION));
        // NOTE: in future, may want to allow client to specify "do not compress"; if so,
        // we would pass Compression.NONE explicitly: null means "try to use whatever"
        if (inputCompression == Compression.NONE) {
            inputCompression = null;
        }
        // TODO: pass in LastAccessUpdateMethod...
        LastAccessUpdateMethod lastAcc = _findLastAccessUpdateMethod(key);

        // assumption here is that we may be passed hash code of orig content, but
        // not that of compressed (latter is easy to calculate on server anyway)
        StorableCreationMetadata stdMetadata = new StorableCreationMetadata(inputCompression,
        		checksum, 0);
        ByteContainer customMetadata = _entryConverter.createMetadata(creationTime, lastAcc,
                ((minTTLSinceAccess == null) ? _cfgDefaultMinTTLSecs : (int) minTTLSinceAccess.getMillis()),
                ((maxTTL == null) ? _cfgDefaultMaxTTLSecs : (int) maxTTL.getMillis())
                );
        StorableCreationResult result;
        try {
            result = _stores.getEntryStore().insert(key.asStorableKey(),
                    dataIn, stdMetadata, customMetadata);
        } catch (StoreException.Input e) { // something client did wrong
            switch (e.getProblem()) {
            case BAD_COMPRESSION:
                return response.badRequest
                        (PutResponse.badArg(key, "Bad Compression information passed: "+e.getMessage()));
            case BAD_CHECKSUM:
                return response.badRequest
                        (PutResponse.badArg(key, "Bad checksum information passed: "+e.getMessage()));
            case BAD_LENGTH:
                return response.badRequest
                        (PutResponse.badArg(key, "Bad length information passed: "+e.getMessage()));
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
            _logDuplicatePut(key);
            // first: will not allow "recreating" a soft-deleted entry
            if (prev.isDeleted()) {
                if (!_cfgAllowUndelete) {
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

    // public for calling from unit tests
    public ServiceResponse removeEntry(ServiceRequest request, ServiceResponse response, K key)
            throws IOException, StoreException
    {
        StorableDeletionResult result = _stores.getEntryStore().softDelete(key.asStorableKey(), true, true);
        /* Even without match, we can claim it is ok... should we?
         * From idempotency perspective, result is that there is no such
         * entry; so let's allow that and just give the usual 204.
         */
        long creationTime = 0L;
        
        // also: if deletion succeeded, may need to delete actual physical file:
        if (result != null && result.hadEntry()) {
            E entry = _entryConverter.entryFromStorable(key, result.getEntry());
            creationTime = entry.getCreationTime();
        }
        return response.ok(new DeleteResponse<K>(key, creationTime));
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
        if (_cfgReportDeletedAsEmpty) {
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
    /* Abstract methods for sub-classes
    /**********************************************************************
     */

    protected abstract LastAccessUpdateMethod _findLastAccessUpdateMethod(K key);
    
    /**
     * Method called to let implementation update last-accessed timestamp if necessary
     * when a piece of content is succesfully fetched with GET (exists and either is
     * not soft-deleted, or passes check for deletion)
     */
    protected abstract void updateLastAccessedForGet(ServiceRequest request, ServiceResponse response,
            E entry, long accessTime);

    protected abstract void updateLastAccessedForHead(ServiceRequest request, ServiceResponse response,
            E entry, long accessTime);
    
    /*
    /**********************************************************************
    /* Error reporting
    /**********************************************************************
     */

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
        if (LOG_DUP_PUTS) {
            LOG.info("Duplicate PUT for key '{}'; success, same checksum", key);
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
    
    protected static Throwable _peel(Throwable t) {
        while (t.getCause() != null) {
                t = t.getCause();
        }
        return t;
    }
}
