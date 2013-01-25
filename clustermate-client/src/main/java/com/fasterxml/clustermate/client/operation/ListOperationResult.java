package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.call.EntryListResult;

/**
 * Result value produced by {@link StoreEntryLister}, contains information
 * about success of individual call, as well as sequence of listed
 * entries in case of successful call.
 */
public class ListOperationResult<T> extends ReadOperationResult<ListOperationResult<T>>
{
    protected EntryListResult<T> result;
    
    public ListOperationResult(OperationConfig config)
    {
        super(config);
    }
}
