package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.api.DecodableRequestPath;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.api.RequestPathStrategy;

/**
 * Implementation used for store tests.
 */
public class PathsForTests extends RequestPathStrategy<PathType>
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
    public <B extends RequestPathBuilder<PathType,B>> B appendPath(B basePath,
            PathType type)
    {
        switch (type) {
        case NODE_METRICS:
            return _nodePath(basePath).addPathSegment(SECOND_SEGMENT_NODE_METRICS);
        case NODE_STATUS:
            return _nodePath(basePath).addPathSegment(SECOND_SEGMENT_NODE_STATUS);

        case STORE_ENTRY:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_ENTRY);
        case STORE_FIND_ENTRY:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_FIND_ENTRY);
        case STORE_FIND_LIST:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_FIND_LIST);
        case STORE_LIST:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_LIST);
        case STORE_STATUS:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_STATUS);

        case SYNC_LIST:
            return _syncPath(basePath).addPathSegment(SECOND_SEGMENT_SYNC_LIST);
        case SYNC_PULL:
            return _syncPath(basePath).addPathSegment(SECOND_SEGMENT_SYNC_PULL);
        }
        throw new IllegalStateException();
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

    protected <B extends RequestPathBuilder<PathType,B>> B _storePath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_STORE);
    }

    protected <B extends RequestPathBuilder<PathType,B>> B _nodePath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_NODE);
    }

    protected <B extends RequestPathBuilder<PathType,B>> B _syncPath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_SYNC);
    }
}
