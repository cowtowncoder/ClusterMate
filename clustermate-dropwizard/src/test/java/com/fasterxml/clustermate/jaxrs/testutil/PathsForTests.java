package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.api.DecodableRequestPath;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.api.RequestPathStrategy;

/**
 * Implementation used for store tests.
 */
@SuppressWarnings("unchecked")
public class PathsForTests extends RequestPathStrategy
{
    
    protected final static String FIRST_SEGMENT_STORE = "teststore";
    protected final static String FIRST_SEGMENT_NODE = "testnode";
    protected final static String FIRST_SEGMENT_SYNC = "testsync";

    protected final static String SECOND_SEGMENT_STORE_ENTRY = "entry";
    protected final static String SECOND_SEGMENT_STORE_LIST = "list";
    protected final static String SECOND_SEGMENT_STORE_STATUS = "status";
    protected final static String SECOND_SEGMENT_STORE_FIND_ENTRY = "findEntry";
    protected final static String SECOND_SEGMENT_STORE_FIND_LIST = "findList";
    
    protected final static String SECOND_SEGMENT_NODE_STATUS = "status";
    protected final static String SECOND_SEGMENT_NODE_METRICS = "metrics";

    protected final static String SECOND_SEGMENT_SYNC_LIST = "list";
    protected final static String SECOND_SEGMENT_SYNC_PULL = "pull";
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Path building
    ///////////////////////////////////////////////////////////////////////
     */
    
    @Override
    public <K extends RequestPathBuilder> K appendStoreEntryPath(K nodeRoot) {
        return (K) _storePath(nodeRoot).addPathSegment(SECOND_SEGMENT_STORE_ENTRY);
    }

    @Override
    public <K extends RequestPathBuilder> K appendStoreListPath(K nodeRoot) {
        return (K) _storePath(nodeRoot).addPathSegment(SECOND_SEGMENT_STORE_LIST);
    }

    @Override
    public <K extends RequestPathBuilder> K appendStoreStatusPath(K nodeRoot) {
        return (K) _storePath(nodeRoot).addPathSegment(SECOND_SEGMENT_STORE_STATUS);
    }

    @Override
    public <K extends RequestPathBuilder> K appendStoreFindEntryPath(K nodeRoot) {
        return (K) _storePath(nodeRoot).addPathSegment(SECOND_SEGMENT_STORE_FIND_ENTRY);
    }

    @Override
    public <K extends RequestPathBuilder> K appendStoreFindListPath(K nodeRoot) {
        return (K) _storePath(nodeRoot).addPathSegment(SECOND_SEGMENT_STORE_FIND_LIST);
    }

    @Override
    public <K extends RequestPathBuilder> K appendNodeStatusPath(K nodeRoot) {
        return (K) _nodePath(nodeRoot).addPathSegment(SECOND_SEGMENT_NODE_STATUS);
    }

    @Override
    public <K extends RequestPathBuilder> K appendNodeMetricsPath(K nodeRoot) {
        return (K) _nodePath(nodeRoot).addPathSegment(SECOND_SEGMENT_NODE_METRICS);
    }

    @Override
    public <K extends RequestPathBuilder> K appendSyncListPath(K nodeRoot) {
        return (K) _syncPath(nodeRoot).addPathSegment(SECOND_SEGMENT_SYNC_LIST);
    }

    @Override
    public <K extends RequestPathBuilder> K appendSyncPullPath(K nodeRoot) {
        return (K) _syncPath(nodeRoot).addPathSegment(SECOND_SEGMENT_SYNC_PULL);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Path matching (decoding)
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public PathType matchPath(DecodableRequestPath pathDecoder)
    {
        String full = pathDecoder.getPath();
        if (pathDecoder.matchPathSegment(FIRST_SEGMENT_STORE)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_ENTRY)) {
                return PathType.STORE_ENTRY;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_LIST)) {
                return PathType.STORE_LIST;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_STATUS)) {
                return PathType.STORE_STATUS;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_FIND_ENTRY)) {
                return PathType.STORE_FIND_ENTRY;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_FIND_LIST)) {
                return PathType.STORE_FIND_LIST;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_NODE)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_NODE_STATUS)) {
                return PathType.NODE_STATUS;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_NODE_METRICS)) {
                return PathType.NODE_METRICS;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_SYNC)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_SYNC_LIST)) {
                return PathType.SYNC_LIST;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_SYNC_PULL)) {
                return PathType.SYNC_PULL;
            }
        }
        // if no match, need to reset
        pathDecoder.setPath(full);
        return null;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected <K extends RequestPathBuilder> K _storePath(K nodeRoot) {
        return (K) nodeRoot.addPathSegment(FIRST_SEGMENT_STORE);
    }

    protected <K extends RequestPathBuilder> K _nodePath(K nodeRoot) {
        return (K) nodeRoot.addPathSegment(FIRST_SEGMENT_NODE);
    }

    protected <K extends RequestPathBuilder> K _syncPath(K nodeRoot) {
        return (K) nodeRoot.addPathSegment(FIRST_SEGMENT_SYNC);
    }
}
