package com.fasterxml.clustermate.client.jdk;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ContentType;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.msg.ItemInfo;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.client.util.ContentConverter;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;
import com.fasterxml.storemate.shared.util.IOUtil;

public class JdkHttpEntryInspector<K extends EntryKey>
    extends BaseJdkHttpAccessor<K>
    implements EntryInspector<K>
{
    protected final ClusterServerNode _server;

    public JdkHttpEntryInspector(StoreClientConfig<K,?> storeConfig,
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
    public <T extends ItemInfo> ReadCallResult<T> tryInspect(CallConfig config,
            ReadCallParameters params, long endOfTime, K contentId, ContentConverter<T> converter)
    {
        if (converter == null) {
            throw new IllegalArgumentException("Missing converter");
        }
        HttpURLConnection conn = null;

        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeoutMsecs = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeoutMsecs < config.getMinimumTimeoutMsecs()) {
            return failed(conn, CallFailure.timeout(_server, startTime, startTime));
        }
        InputStream in = null;

        try {
            JdkHttpClientPathBuilder path = _server.rootPath();
            path = _pathFinder.appendStoreEntryInfoPath(path);
            path = _keyConverter.appendToPath(path, contentId);
            URL url = path.asURL();
            conn = (HttpURLConnection) url.openConnection();

            int statusCode = sendRequest("GET", conn, path, timeoutMsecs);
            handleHeaders(_server, conn, startTime);

            // call ok?
            if (!IOUtil.isHTTPSuccess(statusCode)) {
                if (statusCode == ClusterMateConstants.HTTP_STATUS_NOT_FOUND) { // nothing totally wrong here, present as non-failure
                    return JdkHttpReadCallResult.notFound(_server);
                }
                // if not, why not? Any well-known problems? (besides timeout that was handled earlier)
                String msg = getExcerpt(conn, statusCode, config.getMaxExcerptLength());
                handleHeaders(_server, conn, startTime);
                return failed(conn, CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg));
            }
            ContentType contentType = findContentType(conn, ContentType.JSON);
            in = conn.getInputStream();
            T resp = converter.convert(contentType, in);
            return new JdkHttpReadCallResult<T>(conn, _server, resp);
        } catch (Exception e) {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e2) { }
            }
            return failed(conn, CallFailure.clientInternal(_server,
                    startTime, System.currentTimeMillis(), _unwrap(e)));
        }
    }

    protected <T extends ItemInfo> ReadCallResult<T> failed(HttpURLConnection conn, CallFailure fail) {
        return new JdkHttpReadCallResult<T>(conn, fail);
    }
}

