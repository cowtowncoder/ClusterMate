package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.ClusterServerNode;

/**
 * Result value produced by {@link StoreEntryLister}, contains information
 * about success of individual call, as well as sequence of listed
 * entries in case of successful call.
 */
public class ListResult<T> extends ReadOperationResult<ListResult<T>>
{
    // TODO: result entries
    
    public ListResult(OperationConfig config)
    {
        super(config);
    }

    // BOGUS: just used to get code to compile
    public ListResult<T> setContentLength(ClusterServerNode server, long length)
    {
        return this;
    }
}
