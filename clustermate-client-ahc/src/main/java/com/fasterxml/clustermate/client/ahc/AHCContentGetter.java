package com.fasterxml.clustermate.client.ahc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.impl.StoreClientConfig;

import com.fasterxml.storemate.client.*;
import com.fasterxml.storemate.client.call.CallConfig;
import com.fasterxml.storemate.client.call.ContentGetter;
import com.fasterxml.storemate.client.call.GetCallResult;
import com.fasterxml.storemate.client.call.GetContentProcessor;
import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.HTTPConstants;

import com.ning.http.client.*;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

public class AHCContentGetter<K extends EntryKey>
    extends AHCBasedAccessor<K>
    implements ContentGetter<K>
{
    protected final ClusterServerNode _server;

    public AHCContentGetter(StoreClientConfig<K,?> storeConfig,
            AsyncHttpClient hc, ClusterServerNode server)
    {
        super(storeConfig, hc);
        _server = server;
    }

    /*
    /**********************************************************************
    /* Call implementation
    /**********************************************************************
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
        AHCPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreEntryPath(path);
        path = _keyConverter.appendToPath(path, contentId);       
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

