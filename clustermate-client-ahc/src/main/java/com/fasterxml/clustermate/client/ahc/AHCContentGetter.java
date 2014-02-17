package com.fasterxml.clustermate.client.ahc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.storemate.shared.ByteRange;
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
    public <T> ReadCallResult<T> tryGet(CallConfig config, ReadCallParameters params,
            long endOfTime, K contentId, GetContentProcessor<T> processor, ByteRange range)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeout = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeout < config.getMinimumTimeoutMsecs()) {
            return new AHCReadCallResult<T>(CallFailure.timeout(_server, startTime, startTime));
        }
        AHCPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreEntryPath(path);
        path = _keyConverter.appendToPath(path, contentId);
        if (params != null) {
            path = params.appendToPath(path, contentId);
        }

        // plus, allow use of GZIP and LZF
        path = path.setHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT_COMPRESSION,
                "lzf, gzip, identity");
        // and may use range as well
        if (range != null) {
            path = path.setHeader(ClusterMateConstants.HTTP_HEADER_RANGE_FOR_REQUEST,
                    range.asRequestHeader());
        }

        BoundRequestBuilder reqBuilder = path.getRequest(_httpClient);

        int statusCode = -1;
        UncompressingAsyncHandler<T> handler = new UncompressingAsyncHandler<T>(processor);
        
        try {
            T resp = null;
            ListenableFuture<T> futurama = _httpClient.executeRequest(reqBuilder.build(), handler);
            try {
                resp = futurama.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return new AHCReadCallResult<T>(CallFailure.timeout(_server, startTime, System.currentTimeMillis()));
            }
            statusCode = handler.getStatus();

            handleHeaders(_server, handler.getHeaders(), startTime);
            if (handler.isFailed()) {
                if (statusCode == 404) { // is this a fail or success? For now it's actually success...
                    return AHCReadCallResult.notFound(_server);
                }
                // then the default fallback
                String excerpt = handler.getExcerpt();
                return new AHCReadCallResult<T>(CallFailure.general(_server, statusCode, startTime,
                        System.currentTimeMillis(), excerpt));
            }
            return new AHCReadCallResult<T>(_server, resp);
        } catch (Exception e) {
            return new AHCReadCallResult<T>(CallFailure.clientInternal(_server,
                    startTime, System.currentTimeMillis(), _unwrap(e)));
        }
    }
}

