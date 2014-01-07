package com.fasterxml.clustermate.client.ahc;

import java.io.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.client.CallFailure;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.client.util.ContentConverter;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

public class AHCEntryLister<K extends EntryKey, P extends Enum<P>>
    extends AHCBasedAccessor<K,P>
    implements EntryLister<K>
{
    protected final ClusterServerNode _server;

    public AHCEntryLister(StoreClientConfig<K,?> storeConfig, P endpoint,
            AsyncHttpClient hc,
            ClusterServerNode server)
    {
        super(storeConfig, endpoint, hc);
        _server = server;
    }

    @Override
    public <T> ListCallResult<T> tryList(CallConfig config, long endOfTime,
            K prefix, K lastSeen, ListItemType type, int maxResults,
            ContentConverter<ListResponse<T>> converter)
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
        path = _pathFinder.appendPath(path, _endpoint);
        path = _keyConverter.appendToPath(path, prefix);
        BoundRequestBuilder reqBuilder = path
                .listRequest(_httpClient)
                .addQueryParameter(ClusterMateConstants.QUERY_PARAM_MAX_ENTRIES, String.valueOf(maxResults))
                .addQueryParameter(ClusterMateConstants.QUERY_PARAM_TYPE, type.toString())
                ;
        
        if (lastSeen != null) {
            reqBuilder = reqBuilder
                    .addQueryParameter(ClusterMateConstants.QUERY_PARAM_LAST_SEEN, toBase64(lastSeen.asBytes()));
        }

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
            return new AHCEntryListResult<T>(converter.convert(contentType, in));
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

    protected <T> ListCallResult<T> failed(CallFailure fail) {
        return new AHCEntryListResult<T>(fail);
    }
}
