package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.NodeFailure;

/**
 * Class used for returning information about operation success (or lack thereof).
 */
public interface OperationResult<T extends OperationResult<T>>
{
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */
    
    public OperationConfig getConfig();

    /**
     * Simple accessor for checking whether call succeeded to minimum degree
     * required, or not. This means that we had at least minimal required number
     * of succesful individual calls.
     */
    public boolean succeededMinimally();

    /**
     * Simple accessor for checking whether call succeeded well
     * enough that we may consider it full success.
     * We may either choose to do more updates (if nodes are available);
     * up to {@link #succeededMaximally()} level; or just return
     * and declare success.
     */
    public boolean succeededOptimally();

    /**
     * Simple accessor for checking whether call succeeded as well as it
     * could; meaning that no further calls should be made, even if
     * more nodes were available.
     */
    public boolean succeededMaximally();
    
    public int getFailCount();
    public int getIgnoreCount();

    public int getSuccessCount();

    public Iterable<NodeFailure> getFailures();
    public Iterable<ClusterServerNode> getIgnoredServers();

    public NodeFailure getFirstFail();
}
