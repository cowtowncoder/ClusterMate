package com.fasterxml.clustermate.client.operation;

import java.util.LinkedList;

import com.fasterxml.clustermate.client.ClusterServerNode;

/**
 * Intermediate base class for read operations (GET, HEAD, List).
 * Note that definitions of {@link #failed} and {@link #succeeded()} refer
 * to success of operation itself, but do <b>NOT</b> necessarily mean
 * that content was found: it is possible for operation to succeed but
 * content not to be found (not to exist).
 */
public class ReadOperationResult<T extends ReadOperationResult<T>>
    extends OperationResultImpl<T>
{
    /**
     * Server that successfully delivered content, if any
     */
    protected ClusterServerNode _server;

    /**
     * List of nodes that do not have entry for specified key.
     */
    protected LinkedList<ClusterServerNode> _serversWithoutEntry = null;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected ReadOperationResult(OperationConfig config) {
        super(config);
    }

    /**
     * Method called to indicate that the requested entry was missing from
     * specified server; will return either a new response object, or this
     * response modified with additional information.
     * 
     * @param server Server that was missing requested entry
     */
    @SuppressWarnings("unchecked")
    public T withMissing(ClusterServerNode server)
    {
        if (_serversWithoutEntry == null) {
            _serversWithoutEntry = new LinkedList<ClusterServerNode>();
        }
        _serversWithoutEntry.add(server);
        return (T) this;
    }
    
    /*
    /**********************************************************************
    /* Partial API implementation
    /**********************************************************************
     */
    
    @Override
    public int getSuccessCount() {
        if (_server != null || _serversWithoutEntry != null) {
            return 1;
        }
        return 0;
    }

    @Override
    public boolean succeededMinimally() {
        return getSuccessCount() > 0;
    }

    @Override
    public boolean succeededOptimally() {
        return getSuccessCount() > 0;
    }

    @Override
    public boolean succeededMaximally() {
        return getSuccessCount() > 0;
    }

    @Override
    protected void _addExtraInfo(StringBuilder sb) {
        sb.append(", missing: ").append(getMissingCount());
    }
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    public boolean failed() { return getSuccessCount() == 0; }
    public boolean succeeded() { return getSuccessCount() > 0; }

    public boolean entryFound() { return _server != null; }
    
    public int getMissingCount() {
        return (_serversWithoutEntry == null) ? 0 : _serversWithoutEntry.size();
    }

    public ClusterServerNode getSuccessServer() {
        return _server;
    }
}
