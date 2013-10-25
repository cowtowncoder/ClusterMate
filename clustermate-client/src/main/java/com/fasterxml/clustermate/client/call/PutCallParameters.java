package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.client.StoreClientConfig;

/**
 * Abstract value base class used for passing additional parameters for
 * calls made for PUT operation. Implementations may add more parameterization;
 * and will need to add constructors or methods for creating instances.
 */
public abstract class PutCallParameters
    extends CallParameters
{
    protected PutCallParameters() { super(); }

    protected PutCallParameters(StoreClientConfig<?,?> config) {
        super(config);
    }

    protected PutCallParameters(PutCallParameters base) {
        super(base);
    }

    protected PutCallParameters(PutCallParameters base, StoreClientConfig<?,?> config) {
        super(config);
    }
}
