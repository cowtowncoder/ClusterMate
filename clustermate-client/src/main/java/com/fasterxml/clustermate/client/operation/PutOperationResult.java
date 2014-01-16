package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.call.PutCallParameters;

public class PutOperationResult extends WriteOperationResult<PutOperationResult>
{
    /**
     * Possible parameter overrides in use for this call
     */
    protected final PutCallParameters _params;
    
    public PutOperationResult(OperationConfig config, PutCallParameters params)
    {
        super(config);
        _params = params;
    }

    // generic type to allow for more convenient casting
    @SuppressWarnings("unchecked")
    public <P extends PutCallParameters> P getParams() { return (P) _params; }
}
