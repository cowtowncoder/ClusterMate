package com.fasterxml.clustermate.api;

/**
 * Type that defines how references are built to access
 * a StoreMate service node.
 */
public abstract class RequestPathStrategy
{
    /*
    /**********************************************************************
    /* Methods for building requests paths (by client or server-as-client)
    /**********************************************************************
     */

    /**
     * Method to call to append path segments of specified type, onto
     * partially built base path.
     * 
     * @param basePath Existing path, on to which append specified path segment(s)
     * 
     * @return Path builder after appending specified path
     * 
     * @since 0.9.7
     */
    public abstract <B extends RequestPathBuilder> B appendPath(B basePath,
            PathType type);

    /*
    /**********************************************************************
    /* Methods for decoded requests paths (by server)
    /**********************************************************************
     */
    
    /**
     * Method for finding which entry point given path matches (if any); and if there
     * is a match, consuming matched path.
     */
    public abstract PathType matchPath(DecodableRequestPath pathDecoder);

    /*
    /**********************************************************************
    /* Methods for building requests paths (by client or server-as-client)
    /**********************************************************************
     */

    /**
     * Method for creating the path for accessing stored entries,
     * but without including actual entry id, given a builder that
     * refers to the server node to access
     * 
     * @param nodeRoot Reference to root part of the store node
     * 
     * @return Path for accessing stored entries, not including the actual
     *    entry id.
     */
    @Deprecated
    public <B extends RequestPathBuilder> B appendStoreEntryPath(B nodeRoot) { return null; }

    @Deprecated
    public <B extends RequestPathBuilder> B appendStoreListPath(B nodeRoot) { return null; }

    @Deprecated
    public <B extends RequestPathBuilder> B appendStoreStatusPath(B nodeRoot) { return null; }
    
    @Deprecated
    public <B extends RequestPathBuilder> B appendStoreFindEntryPath(B nodeRoot) { return null; }

    @Deprecated
    public <B extends RequestPathBuilder> B appendStoreFindListPath(B nodeRoot) { return null; }
    
    // // Node status, related:
    
    @Deprecated
    public <B extends RequestPathBuilder> B appendNodeStatusPath(B nodeRoot) { return null; }

    @Deprecated
    public <B extends RequestPathBuilder> B appendNodeMetricsPath(B nodeRoot) { return null; }
    
    // // Sync handling:
    
    @Deprecated
    public <B extends RequestPathBuilder> B appendSyncListPath(B nodeRoot) { return null; }

    @Deprecated
    public <B extends RequestPathBuilder> B appendSyncPullPath(B nodeRoot) { return null; }
}
