package com.fasterxml.clustermate.client.ahc;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.clustermate.api.ContentType;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.msg.ItemInfo;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.client.util.ContentConverter;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.ning.http.client.*;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

public class AHCEntryInspector<K extends EntryKey>
    extends AHCBasedAccessor<K>
    implements EntryInspector<K>
{
    protected final ClusterServerNode _server;

    public AHCEntryInspector(StoreClientConfig<K,?> storeConfig,
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
    public <T extends ItemInfo> ReadCallResult<T> tryInspect(CallConfig config,
            long endOfTime, K contentId, ContentConverter<T> converter)
    {
        if (converter == null) {
            throw new IllegalArgumentException("Missing converter");
        }
        
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeout = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeout < config.getMinimumTimeoutMsecs()) {
            return failed(CallFailure.timeout(_server, startTime, startTime));
        }
        AHCPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreEntryInfoPath(path);
        path = _keyConverter.appendToPath(path, contentId);
        BoundRequestBuilder reqBuilder = path
                .listRequest(_httpClient)
                ;
        InputStream in = null;
        try {
            Future<Response> futurama = _httpClient.executeRequest(reqBuilder.build());
            // First, see if we can get the answer without time out...
            Response resp;
            try {
                resp = futurama.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return failed(CallFailure.timeout(_server, startTime, System.currentTimeMillis()));
            }
            // and if so, is it successful?
            int statusCode = resp.getStatusCode();
            // one thing first: handle standard headers, if any?
            handleHeaders(_server, resp, startTime);

            // call ok?
            if (!IOUtil.isHTTPSuccess(statusCode)) {
                // if not, why not? Any well-known problems? (besides timeout that was handled earlier)

                // then the default fallback
                String msg = getExcerpt(resp, config.getMaxExcerptLength());
                return failed(CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg));
            }
            ContentType contentType = findContentType(resp, ContentType.JSON);
            in = resp.getResponseBodyAsStream();
            T result = converter.convert(contentType, in);
            return new AHCReadCallResult<T>(result);
        } catch (Exception e) {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e2) { }
            }
            return failed(CallFailure.clientInternal(_server,
                    startTime, System.currentTimeMillis(), _unwrap(e)));
        }
    }

    protected <T extends ItemInfo> ReadCallResult<T> failed(CallFailure fail) {
        return new AHCInspectCallResult<T>(fail);
    }
}
