package com.fasterxml.clustermate.client.ahc;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.*;
import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.storemate.shared.util.WithBytesCallback;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.ning.http.client.Body;
import com.ning.http.client.BodyGenerator;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;

/*
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.util.EntityUtils;
*/

/**
 * Helper accessors class used for making a single PUT call to a single
 * server node.
 */
public class AHCContentPutter<K extends EntryKey>
    extends AHCBasedAccessor<K>
    implements ContentPutter<K>
{
    protected final ClusterServerNode _server;

    public AHCContentPutter(StoreClientConfig<K,?> storeConfig,
            AsyncHttpClient asyncHC, ClusterServerNode server)
    {
        super(storeConfig, asyncHC);
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
//          return _tryPutBlocking
            return _tryPutAsync
                    (config, params, endOfTime, contentId, content, startTime, timeout);
        } catch (Exception e) {
            return CallFailure.clientInternal(_server, startTime, System.currentTimeMillis(), e);
        }
    }

    /*
    /**********************************************************************
    /* Implementation: blocking
    /**********************************************************************
     */

    /*
    // With Apache HC:
    public CallFailure _tryPutBlocking(CallConfig config, PutCallParameters params,
    		long endOfTime,
            String contentId, PutContentProvider content,
            final long startTime, final long timeout)
        throws IOException, ExecutionException, InterruptedException, URISyntaxException
    {
        AHCPathBuilder path = _server.rootPath();
        path = _pathFinder.appendPath(path, _endpoint);
        path = _keyConverter.appendToPath(path, contentId);       

        if (params != null) {
        	path = params.appendToPath(path, contentId);
        }

        URIBuilder ub = new URIBuilder(path);
        int checksum = content.getChecksum32();
        addStandardParams(ub, checksum);
        HttpPut put = new HttpPut(ub.build());
        put.setEntity(new InputStreamEntity(content.asStream(), -1L));

        HttpResponse response = _blockingHC.execute(put);
        int statusCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();        
        
        // one more thing: handle standard headers, if any?
//        handleHeaders(_server, resp, startTime);

        if (HttpUtil.isSuccess(statusCode)) {
            EntityUtils.consume(entity);
//            InputStream in = entity.getContent();
//            while (in.skip(Integer.MAX_VALUE) > 0L) { }
//            in.close();
            return null;
        }
        // if not, why not? Any well-known problems?
        // then the default fallback
        String msg = HttpUtil.getExcerpt(EntityUtils.toByteArray(entity));
        return CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg);
    }

//    protected <T extends HttpRequest> T addStandardParams(T request)
    protected URIBuilder addStandardParams(URIBuilder builder,
            int checksum)
    {
        builder.addParameter(Constants.HTTP_QUERY_PARAM_CHECKSUM, 
                (checksum == 0) ? "0" : String.valueOf(checksum));
        return builder;
    }
*/

    /*
    /**********************************************************************
    /* Call implementation
    /**********************************************************************
     */
    
    // And with async-http-client:
    public CallFailure _tryPutAsync(CallConfig config, PutCallParameters params,
    		long endOfTime,
            K contentId, PutContentProvider content,
            final long startTime, final long timeout)
        throws IOException, ExecutionException, InterruptedException
    {
        AHCPathBuilder path = _server.rootPath();
        path = _pathFinder.appendStoreEntryPath(path);
        path = _keyConverter.appendToPath(path, contentId);       
        path = path.setContentType(ClusterMateConstants.HTTP_CONTENT_BINARY);
        if (params != null) {
            path = params.appendToPath(path, contentId);
        }
        // Is compression known?
        Compression comp = content.getExistingCompression();
        if (comp != null) { // if so, must be indicated
            path = path.addCompression(comp, content.uncompressedLength());
        }
        Generator<K> gen = new Generator<K>(content, _keyConverter);
        int checksum = gen.getChecksum();
        path = path.addParameter(ClusterMateConstants.QUERY_PARAM_CHECKSUM,
                (checksum == 0) ? "0" : String.valueOf(checksum));

        BoundRequestBuilder reqBuilder = path.putRequest(_httpClient);
        reqBuilder = reqBuilder.setBody(gen);
        ListenableFuture<Response> futurama = _httpClient.executeRequest(reqBuilder.build());

        // First, see if we can get the answer without time out...
        Response resp;
        try {
            resp = futurama.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return CallFailure.timeout(_server, startTime, System.currentTimeMillis());
        }

        // and if so, is it successful?
        int statusCode = resp.getStatusCode();

        // one more thing: handle standard headers, if any?
        handleHeaders(_server, resp, startTime);

        if (IOUtil.isHTTPSuccess(statusCode)) {
            drain(resp);
            return null;
        }
        // if not, why not? Any well-known problems?

        // then the default fallback
        String msg = getExcerpt(resp, config.getMaxExcerptLength());
        return CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg);
    }

    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */

    protected final static class Generator<K extends EntryKey>
        implements BodyGenerator
    {
        protected final PutContentProvider _content;
        protected final EntryKeyConverter<K> _keyConverter;

        protected final AtomicInteger _checksum;

        public Generator(PutContentProvider content, EntryKeyConverter<K> keyConverter)
        {
            _content = content;
            _keyConverter = keyConverter;
            // Let's see if we can calculate content checksum early, for even the first request
            int checksum = 0;
            ByteContainer bytes = _content.contentAsBytes();
            if (bytes != null) {
                checksum = _keyConverter.contentHashFor(bytes);
            }
            _checksum = new AtomicInteger(checksum);
        }

        public int getChecksum() {
            return _checksum.get();
        }
        
        @Override
        public Body createBody() throws IOException
        {
            int checksum = _checksum.get();
            ByteContainer bytes = _content.contentAsBytes();
            if (bytes != null) {
                if (checksum == 0) {
                    checksum = _keyConverter.contentHashFor(bytes);
                    _checksum.set(checksum);
                }
                return bytes.withBytes(BodyCallback.instance);
            }
            File f = _content.contentAsFile();
            if (f != null) {
                try {
                    return new BodyFileBacked(f, _content.length(), _checksum);
                } catch (IOException ie) {
                    throw new IllegalStateException("Failed to open file '"+f.getAbsolutePath()+"': "
                            +ie.getMessage(), ie);
                }
            }
            // sanity check; we'll never get here:
            throw new IOException("No suitable body generation method found");
        }
    }
    
    protected final static class BodyCallback implements WithBytesCallback<Body>
    {
        public final static BodyCallback instance = new BodyCallback();
        
        @Override
        public Body withBytes(byte[] buffer, int offset, int length) {
            return new BodyByteBacked(buffer, offset, length);
        }
    }
}
