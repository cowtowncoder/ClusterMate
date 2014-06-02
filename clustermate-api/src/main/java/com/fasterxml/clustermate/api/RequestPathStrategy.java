package com.fasterxml.clustermate.api;

/**
 * Type that defines how references are built to access
 * a StoreMate service node.
 *
 * @param <P> Path type enumeration used
 */
public abstract class RequestPathStrategy<P extends Enum <P>>
{
    /*
    /**********************************************************************
    /* Methods for building general requests paths (by client or server-as-client)
    /**********************************************************************å
     */

    /**
     * Method to call to append path segments of specified type, onto
     * partially built base path.
     * 
     * @param basePath Existing path, on to which append specified path segment(s)
     * 
     * @return Path builder after appending specified path
     */
    public abstract <B extends RequestPathBuilder<B>> B appendPath(B basePath, P type);

    /*
    /**********************************************************************
    /* Methods for building basic content access paths
    /**********************************************************************å
     */

    /**
     * Method for building path for accessing payload of specified entry.
     */
    public abstract <B extends RequestPathBuilder<B>> B appendStoreEntryPath(B basePath);

    /**
     * Method for building path for entry point to access item info of individual entries.
     */
    public abstract <B extends RequestPathBuilder<B>> B appendStoreEntryInfoPath(B basePath);

    /**
     * Method for building path for entry point to list available entries with specified
     * path prefix.
     */
    public abstract <B extends RequestPathBuilder<B>> B appendStoreListPath(B basePath);
    
    /*
    /**********************************************************************
    /* Methods for building server-side access paths
    /**********************************************************************å
     */

    public abstract <B extends RequestPathBuilder<B>> B appendSyncListPath(B basePath);

    public abstract <B extends RequestPathBuilder<B>> B appendSyncPullPath(B basePath);

    public abstract <B extends RequestPathBuilder<B>> B appendNodeMetricsPath(B basePath);

    public abstract <B extends RequestPathBuilder<B>> B appendNodeStatusPath(B basePath);

    public abstract <B extends RequestPathBuilder<B>> B appendRemoteSyncListPath(B basePath);

    public abstract <B extends RequestPathBuilder<B>> B appendRemoteSyncPullPath(B basePath);

    public abstract <B extends RequestPathBuilder<B>> B appendRemoteStatusPath(B basePath);
    
    /*
    /**********************************************************************
    /* Methods for decoded requests paths (by server)
    /**********************************************************************
     */
    
    /**
     * Method for finding which entry point given path matches (if any); and if there
     * is a match, consuming matched path.
     */
    public abstract P matchPath(DecodableRequestPath pathDecoder);
}
