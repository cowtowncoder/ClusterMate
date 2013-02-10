package com.fasterxml.clustermate.client.jdk;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.storemate.shared.ByteRange;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.CallFailure;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public class JdkHttpContentGetter<K extends EntryKey>
    extends JdkHttpBasedAccessor<K>
    implements ContentGetter<K>
{
    protected final ClusterServerNode _server;

    public JdkHttpContentGetter(StoreClientConfig<K,?> storeConfig,
            ClusterServerNode server)
    {
        super(storeConfig);
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
            return new JdkHttpGetCallResult<T>(CallFailure.timeout(_server, startTime, startTime));
        }
        JdkHttpClientPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreEntryPath(path);
        path = _keyConverter.appendToPath(path, contentId);       
        BoundRequestBuilder reqBuilder = path.getRequest(_httpClient);
        // plus, allow use of GZIP and LZF
        reqBuilder = reqBuilder.addHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT_COMPRESSION,
                "lzf, gzip, identity");
        // and may use range as well
        if (range != null) {
            reqBuilder = reqBuilder.addHeader(ClusterMateConstants.HTTP_HEADER_RANGE_FOR_REQUEST,
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
                return new JdkHttpGetCallResult<T>(CallFailure.timeout(_server, startTime, System.currentTimeMillis()));
            }
            statusCode = handler.getStatus();

            handleHeaders(_server, handler.getHeaders(), startTime);
            if (handler.isFailed()) {
                if (statusCode == 404) { // is this a fail or success? For now it's actually success...
                    return JdkHttpGetCallResult.notFound();
                }
                // then the default fallback
                return new JdkHttpGetCallResult<T>(CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(),
                        handler.getExcerpt()));
            }
            return new JdkHttpGetCallResult<T>(statusCode, resp);
        } catch (Exception e) {
            return new JdkHttpGetCallResult<T>(CallFailure.clientInternal(_server,
                    startTime, System.currentTimeMillis(), _unwrap(e)));
        }
    }
}

