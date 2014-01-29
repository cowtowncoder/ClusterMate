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
     * @since 0.9.24
     */
    public abstract <B extends RequestPathBuilder<B>> B appendStoreEntryPath(B basePath);

    /**
     * @since 0.9.24
     */
    public abstract <B extends RequestPathBuilder<B>> B appendStoreListPath(B basePath);
    
    /*
    /**********************************************************************
    /* Methods for building server-side access paths
    /**********************************************************************å
     */

    /**
     * @since 0.9.24
     */
    public abstract <B extends RequestPathBuilder<B>> B appendSyncListPath(B basePath);

    /**
     * @since 0.9.24
     */
    public abstract <B extends RequestPathBuilder<B>> B appendSyncPullPath(B basePath);

    /**
     * @since 0.9.24
     */
    public abstract <B extends RequestPathBuilder<B>> B appendNodeMetricsPath(B basePath);

    /**
     * @since 0.9.24
     */
    public abstract <B extends RequestPathBuilder<B>> B appendNodeStatusPath(B basePath);
    
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
