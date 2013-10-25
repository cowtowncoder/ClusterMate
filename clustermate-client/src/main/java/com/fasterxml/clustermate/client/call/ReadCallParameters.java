package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.client.StoreClientConfig;

public abstract class ReadCallParameters
    extends CallParameters
{
    protected ReadCallParameters(StoreClientConfig<?,?> config) {
        super(config);
    }
    
    protected ReadCallParameters(DeleteCallParameters base) {
        super(base);
    }

    protected ReadCallParameters(DeleteCallParameters base, StoreClientConfig<?,?> config) {
        super(config);
    }
}
