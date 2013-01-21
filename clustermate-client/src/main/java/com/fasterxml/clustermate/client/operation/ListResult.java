package com.fasterxml.clustermate.client.operation;

/**
 * Result value produced by {@link ContentLister}, contains information
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

}
