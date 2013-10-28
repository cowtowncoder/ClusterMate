package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.client.StoreClientConfig;

public abstract class ReadCallParameters
    extends CallParameters
{
    protected ReadCallParameters() { super(); }

    protected ReadCallParameters(StoreClientConfig<?,?> config) {
        super(config);
    }
    
    protected ReadCallParameters(ReadCallParameters base) {
        super(base);
    }

    protected ReadCallParameters(ReadCallParameters base, StoreClientConfig<?,?> config) {
        super(config);
    }
}
