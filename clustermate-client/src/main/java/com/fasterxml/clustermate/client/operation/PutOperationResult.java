package com.fasterxml.clustermate.client.operation;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.call.PutCallParameters;

public class PutOperationResult extends OperationResultImpl<PutOperationResult>
{
    /**
     * List of servers for which calls succeeded (possibly after initial failures and re-send),
     * in order of call completion.
     */
    protected final List<ClusterServerNode> _succeeded;

    /**
     * Possible parameter overrides in use for this call
     */
    protected final PutCallParameters _params;
    
    public PutOperationResult(OperationConfig config, PutCallParameters params)
    {
        super(config);
        _succeeded = new ArrayList<ClusterServerNode>(config.getOptimalOks());
        _params = params;
    }

    public PutOperationResult addSucceeded(ClusterServerNode server) {
        _succeeded.add(server);
        return this;
    }

    @Override
    public int getSuccessCount() { return _succeeded.size(); }
    public Iterable<ClusterServerNode> getSuccessServers() { return _succeeded; }

    // generic type to allow for more convenient casting
    @SuppressWarnings("unchecked")
    public <P extends PutCallParameters> P getParams() { return (P) _params; }
}
