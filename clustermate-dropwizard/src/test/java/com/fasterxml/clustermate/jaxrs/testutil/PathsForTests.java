package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.DecodableRequestPath;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.api.RequestPathStrategy;

/**
 * Implementation used for store tests.
 */
public class PathsForTests extends RequestPathStrategy<TestPath>
{
    protected final static String FIRST_SEGMENT_STORE = "teststore";
    protected final static String FIRST_SEGMENT_NODE = "testnode";
    protected final static String FIRST_SEGMENT_SYNC = "testsync";

    protected final static String SECOND_SEGMENT_STORE_ENTRY = "entry";
    protected final static String SECOND_SEGMENT_STORE_ENTRY_INFO = "entryInfo";
    protected final static String SECOND_SEGMENT_STORE_ENTRIES= "entries";
    protected final static String SECOND_SEGMENT_STORE_STATUS = "status";
    protected final static String SECOND_SEGMENT_STORE_FIND_ENTRY = "findEntry";
    protected final static String SECOND_SEGMENT_STORE_FIND_LIST = "findList";
    
    protected final static String SECOND_SEGMENT_NODE_STATUS = "status";
    protected final static String SECOND_SEGMENT_NODE_METRICS = "metrics";

    protected final static String SECOND_SEGMENT_SYNC_LIST = "list";
    protected final static String SECOND_SEGMENT_SYNC_PULL = "pull";
    
    /*
    /**********************************************************************
    /* Path building, generic
    /**********************************************************************
     */
    
    @Override
    public <B extends RequestPathBuilder<B>> B appendPath(B basePath,
            TestPath type)
    {
        switch (type) {
        case NODE_METRICS:
            return _nodePath(basePath).addPathSegment(SECOND_SEGMENT_NODE_METRICS);
        case NODE_STATUS:
            return _nodePath(basePath).addPathSegment(SECOND_SEGMENT_NODE_STATUS);

        case STORE_ENTRY:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_ENTRY);
        case STORE_ENTRY_INFO:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_ENTRY_INFO);
        case STORE_FIND_ENTRY:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_FIND_ENTRY);
        case STORE_FIND_LIST:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_FIND_LIST);
        case STORE_ENTRIES:
            return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_ENTRIES);
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
    /**********************************************************************
    /* Methods for building basic content access paths
    /**********************************************************************å
     */

    @Override
    public <B extends RequestPathBuilder<B>> B appendStoreEntryPath(B basePath) {
        return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_ENTRY);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendStoreEntryInfoPath(B basePath) {
        return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_ENTRY_INFO);
    }
    
    @Override
    public <B extends RequestPathBuilder<B>> B appendStoreListPath(B basePath) {
        return _storePath(basePath).addPathSegment(SECOND_SEGMENT_STORE_ENTRIES);
    }

    /*
    /**********************************************************************
    /* Path building, server-side
    /**********************************************************************
     */

    @Override
    public <B extends RequestPathBuilder<B>> B appendSyncListPath(B basePath) {
        return _syncPath(basePath).addPathSegment(SECOND_SEGMENT_SYNC_LIST);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendSyncPullPath(B basePath) {
        return _syncPath(basePath).addPathSegment(SECOND_SEGMENT_SYNC_PULL);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendNodeStatusPath(B basePath) {
        return _nodePath(basePath).addPathSegment(SECOND_SEGMENT_NODE_STATUS);
    }

    @Override
    public <B extends RequestPathBuilder<B>> B appendNodeMetricsPath(B basePath) {
        return _nodePath(basePath).addPathSegment(SECOND_SEGMENT_NODE_METRICS);
    }
    
    /*
    /**********************************************************************
    /* Path matching (decoding)
    /**********************************************************************
     */

    @Override
    public TestPath matchPath(DecodableRequestPath pathDecoder)
    {
        String full = pathDecoder.getPath();
        if (pathDecoder.matchPathSegment(FIRST_SEGMENT_STORE)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_ENTRY)) {
                return TestPath.STORE_ENTRY;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_ENTRIES)) {
                return TestPath.STORE_ENTRIES;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_STATUS)) {
                return TestPath.STORE_STATUS;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_FIND_ENTRY)) {
                return TestPath.STORE_FIND_ENTRY;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_STORE_FIND_LIST)) {
                return TestPath.STORE_FIND_LIST;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_NODE)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_NODE_STATUS)) {
                return TestPath.NODE_STATUS;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_NODE_METRICS)) {
                return TestPath.NODE_METRICS;
            }
        } else if (pathDecoder.matchPathSegment(FIRST_SEGMENT_SYNC)) {
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_SYNC_LIST)) {
                return TestPath.SYNC_LIST;
            }
            if (pathDecoder.matchPathSegment(SECOND_SEGMENT_SYNC_PULL)) {
                return TestPath.SYNC_PULL;
            }
        }
        // if no match, need to reset
        pathDecoder.setPath(full);
        return null;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected <B extends RequestPathBuilder<B>> B _storePath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_STORE);
    }

    protected <B extends RequestPathBuilder<B>> B _nodePath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_NODE);
    }

    protected <B extends RequestPathBuilder<B>> B _syncPath(B nodeRoot) {
        return nodeRoot.addPathSegment(FIRST_SEGMENT_SYNC);
    }
}
