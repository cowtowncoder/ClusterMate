package com.fasterxml.clustermate.client.jdk;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.CallFailure;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

/**
 * Helper accessors class used for making a single PUT call to a single
 * server node.
 */
public class JdkHttpContentPutter<K extends EntryKey,P extends Enum<P>>
    extends BaseJdkHttpAccessor<K,P>
    implements ContentPutter<K>
{
    protected final ClusterServerNode _server;

    public JdkHttpContentPutter(StoreClientConfig<K,?> storeConfig, P endpoint,
            ClusterServerNode server)
    {
        super(storeConfig, endpoint);
        _server = server;
        _keyConverter = storeConfig.getKeyConverter();
    }

    @Override
    public CallFailure tryPut(CallConfig config, PutCallParameters params,
    		long endOfTime, K contentId, PutContentProvider content)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeout = Math.min(endOfTime - startTime, config.getPutCallTimeoutMsecs());
        if (timeout < config.getMinimumTimeoutMsecs()) {
            return CallFailure.timeout(_server, startTime, startTime);
        }
        try {
            return _tryPut(config, params, endOfTime, contentId, content, startTime, timeout);
        } catch (Exception e) {
            return CallFailure.clientInternal(_server, startTime, System.currentTimeMillis(), e);
        }
    }

    /*
    /**********************************************************************
    /* Blocking implementation
    /**********************************************************************
     */

    public CallFailure _tryPut(CallConfig config, PutCallParameters params,
            long endOfTime,
            K contentId, PutContentProvider content,
            final long startTime, final long timeoutMsecs)
        throws IOException, ExecutionException, InterruptedException
    {
        JdkHttpClientPathBuilder path = _server.rootPath();
        path = _pathFinder.appendPath(path, _endpoint);
        path = _keyConverter.appendToPath(path, contentId);

        // Is compression known?
        Compression comp = content.getExistingCompression();
        if (comp != null) { // if so, must be indicated
            path = path.addCompression(comp, content.uncompressedLength());
        }
        if (params != null) {
            path = params.appendToPath(path, contentId);
        }
        // Ok; and then figure out most optimal way for getting content:

        OutputStream out = null;
        HttpURLConnection conn;
        int hash = content.getContentHash();

        try {
            ByteContainer bc = content.contentAsBytes();
            if (bc != null) { // most efficient, yay
                if (hash == HashConstants.NO_CHECKSUM) {
                    hash = _keyConverter.contentHashFor(bc);
                    content.setContentHash(hash);
                }
                path = addChecksum(path, hash);
                URL url = path.asURL();
                conn = (HttpURLConnection) url.openConnection();
                conn.setDoOutput(true);
                conn.setFixedLengthStreamingMode(bc.byteLength());
                conn = initRequest("PUT", conn, path, timeoutMsecs);
                out = conn.getOutputStream();
                bc.writeBytes(out);
            } else {
                InputStream in; // closed in copy()
                File f = content.contentAsFile();
                // !!! TODO: add wrapper for calculating hash sum, if not yet calculated
                if (f != null) {
                    in = new FileInputStream(f);
                } else {
                    in = content.contentAsStream();
                }
                if (hash != HashConstants.NO_CHECKSUM) {
                    path = addChecksum(path, hash);
                }
                URL url = path.asURL();
                conn = (HttpURLConnection) url.openConnection();
                conn.setDoOutput(true);
                conn.setChunkedStreamingMode(CHUNK_SIZE);
                conn = initRequest("PUT", conn, path, timeoutMsecs);
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
