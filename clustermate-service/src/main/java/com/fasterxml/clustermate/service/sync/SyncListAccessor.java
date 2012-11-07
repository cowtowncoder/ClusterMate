package com.fasterxml.clustermate.service.sync;

import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.VManaged;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.RequestPathBuilder;
import com.fasterxml.storemate.shared.util.IOUtil;

public class SyncListAccessor implements VManaged
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    // public just because tests need it
    public final static String ACCEPTED_CONTENT_TYPES
        = ClusterMateConstants.CONTENT_TYPE_SMILE + ", " + ClusterMateConstants.CONTENT_TYPE_JSON;

    protected final SharedServiceStuff _stuff;
    
//    protected final AsyncHttpClient _asyncHttpClient;
//    protected final HttpClient _blockingHttpClient;
    
    protected final ObjectReader _syncListReader;

    protected final ObjectReader _syncEntryReader;
    
    protected final ObjectWriter _syncPullRequestWriter;

    protected final AtomicBoolean _closed = new AtomicBoolean(false);
    
    public SyncListAccessor(SharedServiceStuff stuff)
    {
        _stuff = stuff;
        _syncListReader = stuff.smileReader(SyncListResponse.class);
        _syncEntryReader = stuff.smileReader(SyncPullEntry.class);
        _syncPullRequestWriter = stuff.jsonWriter(SyncPullRequest.class);

//      _asyncHttpClient = new AsyncHttpClient();
// important: if not using pooled conn manager, must use local instance:
//        _blockingHttpClient = new DefaultHttpClient();
    }

    @Override public void start() { }
    
    @Override
    public void stop()
    {
        _closed.set(true);
//        _asyncHttpClient.close();
//        _blockingHttpClient.getConnectionManager().shutdown();
    }
     
        // quick note: errors are in JSON, data as Smile.
        
        // Old code that uses AHC
        /*
    public SyncListResponse fetchSyncList(IpAndPort endpoint,
            long syncedUpTo, KeyRange syncRange,
            TimeSpan timeout)
        throws InterruptedException
    {
        String url = endpoint.getEndpoint() + VagabondConstants.PATH_SYNC_LIST + "/" + syncedUpTo;
        
        Request req = _asyncHttpClient.prepareGet(url)
                .addQueryParameter(VagabondConstants.HTTP_QUERY_PARAM_KEYRANGE_START, String.valueOf(syncRange.getStart()))
                .addQueryParameter(VagabondConstants.HTTP_QUERY_PARAM_KEYRANGE_LENGTH, String.valueOf(syncRange.getLength()))
                .addHeader(HttpHeaders.ACCEPT, ACCEPTED_CONTENT_TYPES)
                .build();
        // small responses; let's simply buffer, simpler error handling:
        try {
            Future<Response> future = _asyncHttpClient.executeRequest(req);
            Response resp = future.get(timeout.getMillis(), TimeUnit.MILLISECONDS);
            // check status code first:
            int statusCode = resp.getStatusCode();
            byte[] stuff = resp.getResponseBodyAsBytes();
            if (HttpUtil.isSuccess(statusCode)) {
                try {
                    return _syncListReader.readValue(stuff);
                } catch (IOException e) {
                    throw new IOException("Invalid sync list returned by '"+url+"', failed to parse Smile: "+e.getMessage());
                }
            }
            String msg = HttpUtil.getExcerpt(stuff);
            LOG.warn("Failed to send syncList request to '{}': status code {}, response excerpt: {}",
                    new Object[] { url, statusCode, msg});
        } catch (InterruptedException e) {
            throw e;
        } catch (TimeoutException e) {
            LOG.warn("syncList request to {} failed with timeout (of {})", url, timeout);
        } catch (Exception e) {
            LOG.warn("syncList request to {} failed with Exception ({}): {}",
                    new Object[] { url, e.getClass().getName(), e.getMessage()});
        }
        return null;
        */

    // And then the Real Thing, with basic JDK stuff:
    
    public SyncListResponse<?> fetchSyncList(IpAndPort endpoint, TimeSpan timeout,
            long syncedUpTo, KeyRange syncRange)
        throws InterruptedException
    {
        final String urlStr = _buildSyncListUrl(endpoint, syncRange, syncedUpTo);
        HttpURLConnection conn;
        try {
            conn = prepareGet(urlStr, timeout);
            conn.setRequestProperty(ClusterMateConstants.HTTP_HEADER_ACCEPT, ACCEPTED_CONTENT_TYPES);
            conn.connect();
        } catch (Exception e) {
            LOG.warn("fetchSyncList request to {} failed on send with Exception ({}): {}",
                    new Object[] { urlStr, e.getClass().getName(), e.getMessage()});
            return null;
        }
        
        // and if we are good, deal with status code, handle headers/response:
        try {
            int statusCode = conn.getResponseCode();
            if (IOUtil.isHTTPSuccess(statusCode)) {
                InputStream in = conn.getInputStream();
                try {
                    return _syncListReader.readValue(in);
                } catch (IOException e) {
                    throw new IOException("Invalid sync list returned by '"+urlStr+"', failed to parse Smile: "+e.getMessage());
                } finally {
                    try {
                        in.close();
                    } catch (Exception e) { }
                }
            }
            handleHTTPFailure(conn, urlStr, statusCode, "fetchSyncList");
        } catch (Exception e) {
            LOG.warn("syncList request to {} failed on response with Exception ({}): {}",
                    new Object[] { urlStr, e.getClass().getName(), e.getMessage()});
        }            
        return null;

    }

    // Old version with Apache HC:
    /*
    public InputStream readSyncPullResponse(SyncPullRequest request,
            IpAndPort endpoint, AtomicInteger statusCodeWrapper,
            int expectedPayloadSize)
        throws IOException
    {
        String url = endpoint.getEndpoint() + VagabondConstants.PATH_SYNC_PULL;
        byte[] reqPayload = _syncPullRequestWriter.writeValueAsBytes(request);
        
        HttpPost post = new HttpPost(url);
        
//        post.setEntity(new ByteArrayEntity(reqPayload, ContentType.APPLICATION_JSON));
        post.setEntity(new ByteArrayEntity(reqPayload));
        HttpResponse response = _blockingHttpClient.execute(post);
        int statusCode = response.getStatusLine().getStatusCode();
        statusCodeWrapper.set(statusCode);
        HttpEntity entity = response.getEntity();
        
        InputStream in = entity.getContent();
        
        if (!HttpUtil.isSuccess(statusCode)) {
            // try fetching error message?
            String msg = IOUtil.readExcerpt(in, 500);
            in.close();
            LOG.warn("Sync pull failure when requesting {} entries (of about {} mB total payload). Error code {}, response: {}",
                    new Object[] { request.size(), expectedPayloadSize, statusCode, msg});
            return null;
        }
        return in;
    }
    */
    
    public InputStream readSyncPullResponse(SyncPullRequest request, TimeSpan timeout,
            IpAndPort endpoint, AtomicInteger statusCodeWrapper,
            int expectedPayloadSize)
        throws IOException
    {
        final String urlStr = _buildSyncPullUrl(endpoint);
        byte[] reqPayload = _syncPullRequestWriter.writeValueAsBytes(request);

        HttpURLConnection conn;
        OutputStream out = null;
        try {
            conn = preparePost(urlStr, timeout);
            conn.connect();
            out = conn.getOutputStream();
            out.write(reqPayload);
            out.close();
        } catch (Exception e) {
            LOG.warn("readSyncPullResponse request to {} failed on send with Exception ({}): {}",
                    new Object[] { urlStr, e.getClass().getName(), e.getMessage()});
            return null;
        } finally {
            if (out != null) {
                try { out.close(); } catch (IOException e) { }
            }
        }

        try {
            int statusCode = conn.getResponseCode();
            if (IOUtil.isHTTPSuccess(statusCode)) {
                try {
                	return conn.getInputStream();
                } catch (IOException e) {
                    throw new IOException("readSyncPullResponse from '"+urlStr+"' failed: "
                    		+e.getMessage(), e);
                }
            }
            handleHTTPFailure(conn, urlStr, statusCode,
            		"readSyncPullResponse (requesting "+request.size()+" entries (of about "+expectedPayloadSize+" mB total payload)"
            		);
        } catch (Exception e) {
            LOG.warn("syncList request to {} failed on response with Exception ({}): {}",
                    new Object[] { urlStr, e.getClass().getName(), e.getMessage()});
        }            
        return null;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper methpds
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected HttpURLConnection preparePost(String urlStr, TimeSpan timeout)
    	throws IOException
    {
    	return prepareHttpMethod(urlStr, timeout, "POST", true);
    }

    protected HttpURLConnection prepareGet(String urlStr, TimeSpan timeout)
    	throws IOException
    {
    	return prepareHttpMethod(urlStr, timeout, "GET", false);
    }
    
    protected HttpURLConnection prepareHttpMethod(String urlStr, TimeSpan timeout,
    		String methodName, boolean sendInput)
    	throws IOException
	{
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(methodName);
        conn.setAllowUserInteraction(false);
        conn.setUseCaches(false);
        conn.setDoOutput(sendInput);
        conn.setDoInput(true); // we always read response
        // how about timeouts... JDK one does not give us whole-operation granularity but:
        int timeoutMs = (int) timeout.getMillis();
        // let's give only half to connect; more likely we detect down servers on connect
        conn.setConnectTimeout(timeoutMs/2);
        conn.setReadTimeout(timeoutMs);
        return conn;
    }

    // public as it's accessed from outside the package
    public SyncPullEntry decodePullEntry(byte[] data) throws IOException
    {
        return _syncEntryReader.readValue(data);
    }

    protected void handleHTTPFailure(HttpURLConnection conn, String urlStr, int statusCode,
    		String operation)
    {
        String msg = "N/A";
        try {
            InputStream e = conn.getErrorStream();
            msg = IOUtil.getExcerpt(e);
        } catch (Exception e) {
            LOG.warn("Problem reading ErrorStream for failed readSyncPullResponse request to '{}': {}",
                    urlStr, e.getMessage());
        }
        LOG.warn("Failed to process {} response from '{}': status code {}, response excerpt: {}",
                new Object[] { operation, urlStr, statusCode, msg});
    }
    
    protected String _buildSyncListUrl(IpAndPort endpoint, KeyRange syncRange,
            long syncedUpTo)
    {
        final ServiceConfig config = _stuff.getServiceConfig();
        RequestPathBuilder pathBuilder = new JdkHttpClientPathBuilder(endpoint)
            .addPathSegments(config.servicePathRoot);
        pathBuilder = _stuff.getPathStrategy().appendSyncListPath(pathBuilder);
        pathBuilder = pathBuilder.addParameter(ClusterMateConstants.HTTP_QUERY_PARAM_SINCE, String.valueOf(syncedUpTo));
        pathBuilder = pathBuilder.addParameter(ClusterMateConstants.HTTP_QUERY_PARAM_KEYRANGE_START, String.valueOf(syncRange.getStart()));
        pathBuilder = pathBuilder.addParameter(ClusterMateConstants.HTTP_QUERY_PARAM_KEYRANGE_LENGTH, String.valueOf(syncRange.getLength()));
        return pathBuilder.toString();
    }

    protected String _buildSyncPullUrl(IpAndPort endpoint)
    {
        final ServiceConfig config = _stuff.getServiceConfig();
        RequestPathBuilder pathBuilder = new JdkHttpClientPathBuilder(endpoint)
            .addPathSegments(config.servicePathRoot);
        pathBuilder = _stuff.getPathStrategy().appendSyncPullPath(pathBuilder);
        return pathBuilder.toString();
    }
}
