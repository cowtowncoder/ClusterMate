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
import com.fasterxml.clustermate.api.PathType;
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
    public <T> GetCallResult<T> tryGet(CallConfig config, ReadCallParameters params,
            long endOfTime, K contentId, GetContentProcessor<T> processor, ByteRange range)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeoutMsecs = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeoutMsecs < config.getMinimumTimeoutMsecs()) {
            return new JdkHttpGetCallResult<T>(CallFailure.timeout(_server, startTime, startTime));
        }
        try {
            JdkHttpClientPathBuilder path = _server.rootPath();
            path = _pathFinder.appendPath(path, PathType.STORE_ENTRY);
            path = _keyConverter.appendToPath(path, contentId);
            if (params != null) {
                path = params.appendToPath(path, contentId);
            }
            URL url = path.asURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            // plus, allow use of GZIP and LZF
            path = path.setHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT_COMPRESSION,
                    "lzf, gzip, identity");
            // and may use range as well
            if (range != null) {
                path = path.setHeader(ClusterMateConstants.HTTP_HEADER_RANGE_FOR_REQUEST,
                        range.asRequestHeader());
            }
            int statusCode = sendRequest("GET", conn, path, timeoutMsecs);

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
            T result;
            // !!! TODO: compression, iff it's not being uncompressed
            if (h.startContent(statusCode, null)) {
                copy(in, h.asStream(), true);
                result = h.completeContentProcessing();
            } else {
                // need to skip contents, if caller is not interested in them...
                while (in.skip(64000) > 0) { 
                    ;
                }
                result = null;
            }
            return new JdkHttpGetCallResult<T>(conn, statusCode, result);
        } catch (Exception e) {
            return new JdkHttpGetCallResult<T>(CallFailure.clientInternal(_server,
                    startTime, System.currentTimeMillis(), _unwrap(e)));
        }
    }
}

