package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.StoreClientConfig;

/**
 * Abstract value base class used for passing additional parameters for
 * calls made for PUT operation. Implementations may add more parameterization;
 * and will need to add constructors or methods for creating instances.
 */
public abstract class PutCallParameters
    extends CallParameters
{
    protected PutCallParameters(StoreClientConfig<?,?> config) {
        super(config);
    }

    protected PutCallParameters(PutCallParameters base) {
        super(base);
    }

    protected PutCallParameters(PutCallParameters base, StoreClientConfig<?,?> config) {
        super(config);
    }
    
    /**
     * Method called by low-level HTTP client to add parameters into request
     * path (and related information, like headers).
     * 
     * @param pathBuilder builder used for building request path
     * @param contentId If of the entry being put; usually not directly needed (already appended to path),
     *   but may be needed by some implementations
     */
    public abstract <B extends RequestPathBuilder> B appendToPath(B pathBuilder, EntryKey contentId);
}
