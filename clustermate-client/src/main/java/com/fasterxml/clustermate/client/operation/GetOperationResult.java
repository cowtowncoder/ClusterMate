package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.call.GetContentProcessor;

/**
 * {@link OperationResult} subtype used with GET operations, adds actual
 * result (of type <code>T</code> which is type of {@link GetContentProcessor}
 * passed to call).
 * 
 * @param <T> Result type of {@link GetContentProcessor}
 */
public class GetOperationResult<T> extends ReadOperationResult<GetOperationResult<T>>
{
    /**
     * Actual contents successfully fetched, if any.
     */
    protected T _contents;

    public GetOperationResult(OperationConfig config) {
        super(config);
    }

    public GetOperationResult<T> setContents(ClusterServerNode server, T contents)
    {
        if (_server != null) {
            throw new IllegalStateException("Already received successful response from "+_server+"; trying to override with "+server);
        }
        _server = server;
        _contents = contents;
        return this;
    }
    
    // // // Extended API

    public T getContents() { return _contents; }
}

