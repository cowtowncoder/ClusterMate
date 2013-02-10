package com.fasterxml.clustermate.client.jdk;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.CallFailure;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.util.IOUtil;

/**
 * Helper accessors class used for making a single PUT call to a single
 * server node.
 */
public class JdkHttpContentPutter<K extends EntryKey>
    extends BaseJdkHttpAccessor<K>
    implements ContentPutter<K>
{
    protected final ClusterServerNode _server;

    public JdkHttpContentPutter(StoreClientConfig<K,?> storeConfig,
            ClusterServerNode server)
    {
        super(storeConfig);
        _server = server;
        _keyConverter = storeConfig.getKeyConverter();
    }

    @Override
    public CallFailure tryPut(CallConfig config, long endOfTime,
            K contentId, PutContentProvider content)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeout = Math.min(endOfTime - startTime, config.getPutCallTimeoutMsecs());
        if (timeout < config.getMinimumTimeoutMsecs()) {
            return CallFailure.timeout(_server, startTime, startTime);
        }
        try {
            return _tryPut(config, endOfTime, contentId, content, startTime, timeout);
        } catch (Exception e) {
            return CallFailure.clientInternal(_server, startTime, System.currentTimeMillis(), e);
        }
    }

    /*
    /**********************************************************************
    /* Blocking implementation
    /**********************************************************************
     */

    @SuppressWarnings("resource")
    public CallFailure _tryPut(CallConfig config, long endOfTime,
            K contentId, PutContentProvider content,
            final long startTime, final long timeout)
        throws IOException, ExecutionException, InterruptedException
    {
        JdkHttpClientPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreEntryPath(path);
        path = _keyConverter.appendToPath(path, contentId);       
        /*
        BoundRequestBuilder reqBuilder = path.putRequest(_httpClient);
        Generator<K> gen = new Generator<K>(content, _keyConverter);
        int checksum = gen.getChecksum();
        reqBuilder = addCheckSum(reqBuilder, checksum);
        reqBuilder = reqBuilder.setBody(gen);
        */

        // Minor optimization; only using chunking if necessary

        // Ok; and then figure out most optimal way for getting content:

        OutputStream out = null;
        HttpURLConnection conn;

        try {
            ByteContainer bc = content.contentAsBytes();
            if (bc != null) { // most efficient, yay
                int checksum = _keyConverter.contentHashFor(bc);
                path = addChecksum(path, checksum);
                URL url = path.asURL();
                conn = (HttpURLConnection) url.openConnection();
                conn.setFixedLengthStreamingMode(bc.byteLength());
                conn.setRequestMethod("PUT");
                out = conn.getOutputStream();
                bc.writeBytes(out);
            } else {
                InputStream in; // closed in copy()
                File f = content.contentAsFile();
                if (f != null) {
                    in = new FileInputStream(f);
                } else {
                    in = content.contentAsStream();
                }
                URL url = path.asURL();
                conn = (HttpURLConnection) url.openConnection();
                conn.setChunkedStreamingMode(CHUNK_SIZE);
                conn.setRequestMethod("PUT");
                out = conn.getOutputStream();
                copy(in, out, true);
            }
        } finally {
            if (out != null) {
                try { out.close(); } catch (IOException e) {
                    logWarn("Problems closing stream: "+e.getMessage());
                }
            }
        }
        int statusCode = conn.getResponseCode();

        // one more thing: handle standard headers, if any?
        handleHeaders(_server, conn, startTime);

        if (IOUtil.isHTTPSuccess(statusCode)) {
            drain(conn, statusCode);
            return null;
        }
        // if not, why not? Any well-known problems?

        // then the default fallback
        String msg = getExcerpt(conn, statusCode, config.getMaxExcerptLength());
        return CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg);
    }
}
