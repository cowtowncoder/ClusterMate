package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.NodeFailure;
import com.fasterxml.clustermate.client.call.CallFailure;

/**
 * Container used to hold in-flight information about calls to a single applicable
 * target node.
 */
final class SingleCallState
{
    protected final ClusterServerNode _node;

    protected NodeFailure _fails;
    
    public SingleCallState(ClusterServerNode node)
    {
        _node = node;
    }

    public void addFailure(CallFailure fail) {
        if (_fails == null) {
            _fails = new NodeFailure(_node, fail);
        } else {
            _fails.addFailure(fail);
        }
    }
    
    public ClusterServerNode server() { return _node; }

    public NodeFailure getFails() { return _fails; }
}