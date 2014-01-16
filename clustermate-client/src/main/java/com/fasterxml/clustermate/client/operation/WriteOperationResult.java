package com.fasterxml.clustermate.client.operation;

import java.util.*;

import com.fasterxml.clustermate.client.ClusterServerNode;

/**
 * Intermediate base class for read operations (GET, HEAD, List).
 * Note that definitions of {@link #failed} and {@link #succeeded()} refer
 * to success of operation itself, but do <b>NOT</b> necessarily mean
 * that content was found: it is possible for operation to succeed but
 * content not to be found (not to exist).
 */
public class WriteOperationResult<T extends WriteOperationResult<T>>
    extends OperationResultImpl<T>
{
    /**
     * List of servers for which calls succeeded (possibly after initial failures and re-send),
     * in order of call completion.
     */
    protected final List<ClusterServerNode> _succeeded;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected WriteOperationResult(OperationConfig config) {
        super(config);
        _succeeded = new ArrayList<ClusterServerNode>(config.getOptimalOks());
   }

    @SuppressWarnings("unchecked")
    public T addSucceeded(ClusterServerNode server) {
        _succeeded.add(server);
        return (T) this;
    }
    
    /*
    /**********************************************************************
    /* Partial API implementation
    /**********************************************************************
     */
    
    @Override
    public final int getSuccessCount() { return _succeeded.size(); }

    @Override
    public boolean succeededMinimally() {
        return getSuccessCount() >= _config.getMinimalOksToSucceed();
    }

    @Override
    public boolean succeededOptimally() {
        return getSuccessCount() >= _config.getOptimalOks();
    }

    @Override
    public boolean succeededMaximally() {
        return getSuccessCount() >= _config.getMaxOks();
    }
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    public Iterable<ClusterServerNode> getSuccessServers() { return _succeeded; }
}
