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
     * Method for creating the path for accessing stored entries,
     * but without including actual entry id, given a builder that
     * refers to the server node to access
     * 
     * @param nodeRoot Reference to root part of the store node
     * 
     * @return Path for accessing stored entries, not including the actual
     *    entry id.
     */
    public abstract <B extends RequestPathBuilder> B appendStoreEntryPath(B nodeRoot);

    public abstract <B extends RequestPathBuilder> B appendStoreListPath(B nodeRoot);

    // // Node status, related:
    
    public abstract <B extends RequestPathBuilder> B appendNodeStatusPath(B nodeRoot);
    
    // // Sync handling:
    
    public abstract <B extends RequestPathBuilder> B appendSyncListPath(B nodeRoot);

    public abstract <B extends RequestPathBuilder> B appendSyncPullPath(B nodeRoot);

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
}
