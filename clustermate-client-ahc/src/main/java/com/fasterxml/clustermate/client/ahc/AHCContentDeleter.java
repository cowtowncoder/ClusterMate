package com.fasterxml.clustermate.client.ahc;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.CallConfig;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.ContentDeleter;
import com.fasterxml.clustermate.client.call.DeleteCallParameters;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

public class AHCContentDeleter<K extends EntryKey>
    extends AHCBasedAccessor<K>
    implements ContentDeleter<K>
{
    public AHCContentDeleter(StoreClientConfig<K,?> storeConfig,
            AsyncHttpClient hc, ClusterServerNode server)
    {
        super(storeConfig, hc, server);
    }

    @Override
    public CallFailure tryDelete(CallConfig config, DeleteCallParameters params,
            long endOfTime, K contentId)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeout = Math.min(endOfTime - startTime, config.getDeleteCallTimeoutMsecs());
        if (timeout < config.getMinimumTimeoutMsecs()) {
            return CallFailure.timeout(_server, startTime, startTime);
        }
        AHCPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreEntryPath(path);
        path = _keyConverter.appendToPath(path, contentId);    	
        if (params != null) {
            path = params.appendToPath(path, contentId);
        }
        BoundRequestBuilder reqBuilder = path.deleteRequest(_httpClient);

        try {
            Future<Response> futurama = _httpClient.executeRequest(reqBuilder.build());
            // First, see if we can get the answer without time out...
            Response resp;
            try {
                resp = futurama.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return CallFailure.timeout(_server, startTime, System.currentTimeMillis());
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
                return CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg);
            }
            return null;
        } catch (Exception e) {
            return failFromException(e, startTime);
        }
    }
}
