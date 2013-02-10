package com.fasterxml.clustermate.client.jdk;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.storemate.shared.util.IOUtil;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.client.util.ContentConverter;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public class JdkHttpEntryLister<K extends EntryKey>
    extends BaseJdkHttpAccessor<K>
    implements EntryLister<K>
{
    protected final ClusterServerNode _server;

    public JdkHttpEntryLister(StoreClientConfig<K,?> storeConfig,
            ClusterServerNode server)
    {
        super(storeConfig);
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
        final long timeoutMsecs = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeoutMsecs < config.getMinimumTimeoutMsecs()) {
            return failed(CallFailure.timeout(_server, startTime, startTime));
        }
        
        JdkHttpClientPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreListPath(path);
        path = _keyConverter.appendToPath(path, prefix);
        path.addParameter(ClusterMateConstants.QUERY_PARAM_MAX_ENTRIES, String.valueOf(maxResults))
                .addParameter(ClusterMateConstants.QUERY_PARAM_TYPE, type.toString())
                ;
        if (lastSeen != null) {
            path = path.addParameter(ClusterMateConstants.QUERY_PARAM_LAST_SEEN, toBase64(lastSeen.asBytes()));
        }
        InputStream in = null;

        try {
            URL url = path.asURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            setTimeouts(conn, timeoutMsecs);
            conn.setRequestMethod("GET");
            conn.setChunkedStreamingMode(CHUNK_SIZE);
            int statusCode = conn.getResponseCode();
            handleHeaders(_server, conn, startTime);

            // call ok?
            if (!IOUtil.isHTTPSuccess(statusCode)) {
                // if not, why not? Any well-known problems? (besides timeout that was handled earlier)

                // then the default fallback
                String msg = getExcerpt(conn, statusCode, config.getMaxExcerptLength());
                return failed(CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg));
            }
            ContentType contentType = findContentType(conn, ContentType.JSON);
            in = conn.getInputStream();
            return new JdkHttpEntryListResult<T>(conn, converter.convert(contentType, in));
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
        return new JdkHttpEntryListResult<T>(fail);
    }
}
