package com.fasterxml.clustermate.client.jdk;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.util.IOUtil;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.client.call.GetContentProcessor.Handler;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public class JdkHttpContentGetter<K extends EntryKey>
    extends BaseJdkHttpAccessor<K>
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
        final long timeoutMsecs = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeoutMsecs < config.getMinimumTimeoutMsecs()) {
            return new JdkHttpGetCallResult<T>(CallFailure.timeout(_server, startTime, startTime));
        }
        try {
            JdkHttpClientPathBuilder path = _server.rootPath();
            path = _pathFinder.appendStoreEntryPath(path);
            path = _keyConverter.appendToPath(path, contentId);
            URL url = path.asURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            // plus, allow use of GZIP and LZF
            conn.setRequestProperty(ClusterMateConstants.HTTP_HEADER_ACCEPT_COMPRESSION,
                    "lzf, gzip, identity");
            // and may use range as well
            if (range != null) {
                conn.setRequestProperty(ClusterMateConstants.HTTP_HEADER_RANGE_FOR_REQUEST,
                         range.asRequestHeader());
            }

            setTimeouts(conn, timeoutMsecs);
            int statusCode = conn.getResponseCode();
            // one thing first: handle standard headers, if any?
            handleHeaders(_server, conn, startTime);

            if (!IOUtil.isHTTPSuccess(statusCode)) {
                if (statusCode == 404) { // is this a fail or success? For now it's actually success...
                    return JdkHttpGetCallResult.notFound();
                }
                // then the default fallback
                return new JdkHttpGetCallResult<T>(CallFailure.general(_server, statusCode, startTime,
                         System.currentTimeMillis(), getExcerpt(conn, statusCode, config.getMaxExcerptLength())));
            }

            InputStream in = conn.getInputStream();
            // Then, anything to uncompress?
            String comps = conn.getHeaderField(ClusterMateConstants.HTTP_HEADER_COMPRESSION);
            if (comps != null && !comps.isEmpty()) {
                Compression comp = Compression.from(comps);
                if (comp != null) {
                    in = Compressors.uncompressingStream(in, comp);
                }
            }
            Handler<T> h = processor.createHandler();
            copy(in, h.asStream(), true);
            T result = h.completeContentProcessing();
            return new JdkHttpGetCallResult<T>(conn, statusCode, result);
        } catch (Exception e) {
            return new JdkHttpGetCallResult<T>(CallFailure.clientInternal(_server,
                    startTime, System.currentTimeMillis(), _unwrap(e)));
        }
    }
}

