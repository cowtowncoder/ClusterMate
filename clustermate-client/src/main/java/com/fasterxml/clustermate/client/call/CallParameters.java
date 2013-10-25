package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.StoreClientConfig;

/**
 * Common base class for optional parameters that can be passed for
 * calls and operations.
 */
public abstract class CallParameters
{
    protected final StoreClientConfig<?,?> _clientConfig;
    
    protected CallParameters(StoreClientConfig<?,?> config) {
        _clientConfig = config;
    }

    protected CallParameters(CallParameters src) {
        if (src == null) {
            _clientConfig = null;
        } else {
            _clientConfig =  src._clientConfig;
        }
    }

    /*
    /**********************************************************************
    /* Mutant factories
    /**********************************************************************
     */

    /**
     * Mutant factory that will return an instance that is configured
     * with given {@link StoreClientConfig} instance.
     */
    public abstract CallParameters withClientConfig(StoreClientConfig<?,?> config);
    
    /*
    /**********************************************************************
    /* Simple accessors
    /**********************************************************************
     */
    
    public StoreClientConfig<?,?> getClientConfig() {
        return _clientConfig;
    }

    /*
    /**********************************************************************
    /* Other methods
    /**********************************************************************
     */
    
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
