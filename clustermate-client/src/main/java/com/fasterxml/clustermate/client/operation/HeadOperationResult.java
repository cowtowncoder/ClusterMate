package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.ClusterServerNode;

public class HeadOperationResult
    extends ReadOperationResult<HeadOperationResult>
{
    /**
     * Actual length fetched, if any
     */
    protected long _contentLength = -1L;
    
    public HeadOperationResult(OperationConfig config)
    {
        super(config);
    }
    
    public HeadOperationResult setContentLength(ClusterServerNode server, long length)
    {
        if (_server != null) {
            throw new IllegalStateException("Already received successful response from "+_server+"; trying to override with "+server);
        }
        _server = server;
        _contentLength = length;
        return this;
    }

    // // // Extended API

    public long getContentLength() { return _contentLength; }
}
