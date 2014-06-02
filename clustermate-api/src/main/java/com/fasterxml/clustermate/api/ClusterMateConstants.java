package com.fasterxml.clustermate.api;

/**
 * Constants for ClusterMate and systems built on it.
 */
public interface ClusterMateConstants
{
    /*
    /**********************************************************************
    /* Standard HTTP Headers
    /**********************************************************************
     */

    /**
     * Standard HTTP header indicating type of content (payload) of the
     * response/request.
     */
    public final static String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
    
    /**
     * Standard HTTP header indicating compression that Entity (payload) uses,
     * if any.
     */
    public final static String HTTP_HEADER_COMPRESSION = "Content-Encoding";

    /**
     * Standard HTTP header indicating content type(s) caller accepts
     */
    public final static String HTTP_HEADER_ACCEPT = "Accept";
    
    /**
     * Standard HTTP header indicating compression methods client accepts,
     * if any.
     */
    public final static String HTTP_HEADER_ACCEPT_COMPRESSION = "Accept-Encoding";

    /**
     * Standard HTTP header that indicates length of entity in bytes, if length
     * is known; missing or -1 if not known.
     */
    public final static String HTTP_HEADER_CONTENT_LENGTH = "Content-Length";
    
    public final static String HTTP_HEADER_RANGE_FOR_REQUEST = "Range";
    
    public final static String HTTP_HEADER_RANGE_FOR_RESPONSE = "Content-Range";

    // // Support for Etag-based caching
    
    public final static String HTTP_HEADER_ETAG = "Etag";

    public final static String HTTP_HEADER_ETAG_NO_MATCH = "If-None-Match";
    
    /*
    /**********************************************************************
    /* Custom HTTP Headers
    /**********************************************************************
     */
    
    /**
     * Name of custom HTTP header we use to indicate the latest
     * available cluster state update from given server node.
     *<p>
     * Whether to use "X-" prefix or not seems in dispute; can change
     * if need be.
     */
    public final static String CUSTOM_HTTP_HEADER_LAST_CLUSTER_UPDATE = "X-CM-ClusterUpdate";

    /**
     * In case of PUT that does not provide checksum as argument, server may
     * return checksum upon successful call. This can be used for further
     * calls by client.
     */
    public final static String CUSTOM_HTTP_HEADER_CHECKSUM = "X-CM-Checksum";

    /**
     * When specifying {@link #HTTP_HEADER_COMPRESSION} for values other than "none",
     * it is necessary to also indicate the original content length
     */
    public final static String CUSTOM_HTTP_HEADER_UNCOMPRESSED_LENGTH = "X-CM-UncompressedLength";

    /*
    /**********************************************************************
    /* Query parameters, ClusterMate-specific
    /**********************************************************************
     */

    /**
     * When passing various checksums with GET, can use this query parameter.
     */
    public final static String QUERY_PARAM_CHECKSUM = "checksum";

    /**
     * @deprecated Should promote this out of ClusterMate; not needed by all impls
     */
    @Deprecated
    public final static String QUERY_PARAM_MIN_SINCE_ACCESS_TTL = "minSinceAccessTTL";

    public final static String QUERY_PARAM_MAX_TTL = "maxTTL";
    
    /**
     * Query parameter used for defining timestamp after which (inclusive) entries are
     * to be returned, as determine by their insertion time.
     */
    public final static String QUERY_PARAM_SINCE = "since";
    
    public final static String QUERY_PARAM_KEYRANGE_START = "keyRangeStart";

    public final static String QUERY_PARAM_KEYRANGE_LENGTH = "keyRangeLength";

    /**
     * Query parameter used to pass information about calling node.
     */
    public final static String QUERY_PARAM_CALLER = "caller";

    /**
     * Query parameter that indicates state of the caller
     */
    public final static String QUERY_PARAM_STATE = "state";
    
    /**
     * Query parameter used to contain hash code for last received cluster
     * view.
     */
    public final static String QUERY_PARAM_CLUSTER_HASH = "clusterHash";

    /**
     * Query parameter that contains timestamp defined by sender and indicates
     * timestamp relevant to the message (such as status update).
     */
    public final static String QUERY_PARAM_TIMESTAMP = "timestamp";

    /**
     * Query parameter that defines type of items/entries to list.
     */
    public final static String QUERY_PARAM_TYPE = "type";

    /**
     * Query parameter that defines maximum number of entries caller wishes
     * to list.
     */
    public final static String QUERY_PARAM_MAX_ENTRIES = "maxEntries";

    /**
     * Query parameter that defines what was the last traversed item, if any,
     * when listing entries.
     */
    public final static String QUERY_PARAM_LAST_SEEN = "lastSeen";

    /**
     * Number of retries for this particular request; used mostly with entry
     * points that re-route, and typically do round-robin dispatch using
     * this value
     */
    public final static String QUERY_PARAM_RETRY_COUNT = "retry";

    /*
    /**********************************************************************
    /* Standard HTTP Response codes
    /**********************************************************************
     */

    public final static int HTTP_STATUS_OK = 200;

    public final static int HTTP_STATUS_OK_PARTIAL = 206;

    public final static int HTTP_STATUS_NOT_FOUND = 404;

    public final static int HTTP_STATUS_ERROR_CONFLICT = 409;

    public final static int HTTP_STATUS_ERROR_GONE = 410;
    
    /*
    /**********************************************************************
    /* Custom 'virtual' HTTP response codes; used to indicate client-side
    /* failures that do not have real server-provided failing status code
    /**********************************************************************
     */
    
    /**
     * Constant used as a placeholder for internal call failure caused
     * by an exception thrown on client side.
     */
    public final static int HTTP_STATUS_CUSTOM_FAIL_CLIENT_THROWABLE = -2;

    /**
     * Constant used as a placeholder for internal call failure caused
     * by a problem on client side (before trying to call server),
     * as identified by given message
     */
    public final static int HTTP_STATUS_CUSTOM_FAIL_CLIENT_MESSAGE = -3;

    /**
     * More specialized variant that stems from a {@link NullPointerException} on
     * client side, before or during processing of the request, but before
     * getting a real HTTP response code to use.
     * 
     * @since 0.10.4
     */
    public final static int HTTP_STATUS_CUSTOM_FAIL_CLIENT_NPE = -4;

    /* Response code used when the request timed out; as per docs, while
     * not a formally standardized code, is actually used. And is considered
     * retriable (as 5xx code) which is why we choose it.
     */
    public final static int HTTP_STATUS_CLIENT_TIMEOUT_ON_READ = 598;    

    /*
    /**********************************************************************
    /* Other constants
    /**********************************************************************
     */

    public final static String STATE_ACTIVE = "active";

    public final static String STATE_INACTIVE = "inactive";

    public final static String HTTP_CONTENT_BINARY = "application/octet-stream";
}
