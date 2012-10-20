package com.fasterxml.clustermate.client.ahc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.clustermate.client.cluster.ClusterServerNode;

import com.fasterxml.storemate.api.ByteRange;
import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.HTTPConstants;
import com.fasterxml.storemate.client.*;
import com.fasterxml.storemate.client.call.CallConfig;
import com.fasterxml.storemate.client.call.ContentGetter;
import com.fasterxml.storemate.client.call.GetCallResult;
import com.fasterxml.storemate.client.call.GetContentProcessor;

import com.ning.http.client.*;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

public class AHCContentGetter<K extends EntryKey>
    extends AHCBasedAccessor
    implements ContentGetter<K>
{
    protected final ClusterServerNode _server;

    public AHCContentGetter(AsyncHttpClient hc, ObjectMapper m,
            ClusterServerNode server)
    {
        super(hc, m);
        _server = server;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // External API
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public <T> GetCallResult<T> tryGet(CallConfig config, long endOfTime,
    		K contentId, GetContentProcessor<T> processor,
    		ByteRange range)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeout = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeout < config.getMinimumTimeoutMsecs()) {
            return new AHCGetCallResult<T>(CallFailure.timeout(_server, startTime, startTime));
        }
    	AHCPathBuilder path = _server.resourceEndpoint();
    	path = contentId.appendToPath(path);    	
    	BoundRequestBuilder reqBuilder = path.getRequest(_httpClient);
        // plus, allow use of GZIP and LZF
        reqBuilder = reqBuilder.addHeader(HTTPConstants.HTTP_HEADER_ACCEPT_COMPRESSION,
                "lzf, gzip, identity");
        // and may use range as well
        if (range != null) {
            reqBuilder = reqBuilder.addHeader(HTTPConstants.HTTP_HEADER_RANGE_FOR_REQUEST,
            		range.asRequestHeader());
        }
        
        int statusCode = -1;
        UncompressingAsyncHandler<T> handler = new UncompressingAsyncHandler<T>(processor);
        
        try {
            T resp = null;
            ListenableFuture<T> futurama = _httpClient.executeRequest(reqBuilder.build(), handler);
            try {
                resp = futurama.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return new AHCGetCallResult<T>(CallFailure.timeout(_server, startTime, System.currentTimeMillis()));
            }
            statusCode = handler.getStatus();

            handleHeaders(_server, handler.getHeaders(), startTime);
            if (handler.isFailed()) {
                if (statusCode == 404) { // is this a fail or success? For now it's actually success...
                    return AHCGetCallResult.notFound();
                }
                // then the default fallback
                return new AHCGetCallResult<T>(CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(),
                        handler.getExcerpt()));
            }
            return new AHCGetCallResult<T>(statusCode, resp);
        } catch (Exception e) {
            Throwable t = e;
            while (t.getCause() != null) {
                t = t.getCause();
            }
            return new AHCGetCallResult<T>(CallFailure.internal(_server, startTime, System.currentTimeMillis(), t));
        }
    }
}

