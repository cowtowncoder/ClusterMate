package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.call.DeleteCallParameters;

public class DeleteOperationResult extends WriteOperationResult<DeleteOperationResult>
{
    /**
     * Possible parameter overrides in use for this call
     */
    protected final DeleteCallParameters _params;

    public DeleteOperationResult(OperationConfig config, DeleteCallParameters params) {
        super(config);
        _params = params;
    }
}
