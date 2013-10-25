package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.client.StoreClientConfig;

public abstract class DeleteCallParameters
    extends CallParameters
{
    protected DeleteCallParameters() { super(); }

    protected DeleteCallParameters(StoreClientConfig<?,?> config) {
        super(config);
    }

    protected DeleteCallParameters(DeleteCallParameters base) {
        super(base);
    }

    protected DeleteCallParameters(DeleteCallParameters base, StoreClientConfig<?,?> config) {
        super(config);
    }
}
