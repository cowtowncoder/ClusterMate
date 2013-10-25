package com.fasterxml.clustermate.client.call;

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
    
    public StoreClientConfig<?,?> getClientConfig() {
        return _clientConfig;
    }
    
    /**
     * Mutant factory that will return an instance that is configured
     * with given {@link StoreClientConfig} instance.
     */
    public abstract CallParameters withClientConfig(StoreClientConfig<?,?> config);
}
