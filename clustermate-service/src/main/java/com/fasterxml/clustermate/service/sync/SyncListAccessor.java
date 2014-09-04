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
import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerUpdatable;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.IOUtil;

import static com.fasterxml.clustermate.api.ClusterMateConstants.*;

public class SyncListAccessor
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    // public just because tests need it
    public final static String ACCEPTED_CONTENT_TYPES
        = ContentType.SMILE.toString() + ", " + ContentType.JSON.toString();

    protected final SharedServiceStuff _stuff;

    protected final RequestPathStrategy<?> _pathStrategy;
    
//    protected final AsyncHttpClient _asyncHttpClient;
//    protected final HttpClient _blockingHttpClient;
    
    protected final ObjectReader _syncListReader;

    protected final ObjectReader _syncEntryReader;
    
    protected final ObjectWriter _syncPullRequestWriter;

    protected final AtomicBoolean _closed = new AtomicBoolean(false);
    
    public SyncListAccessor(SharedServiceStuff stuff)
    {
        _stuff = stuff;
        _pathStrategy = _stuff.getPathStrategy();
        _syncListReader = stuff.smileReader(SyncListResponse.class);
        _syncEntryReader = stuff.smileReader(SyncPullEntry.class);
        _syncPullRequestWriter = stuff.jsonWriter(SyncPullRequest.class);

//      _asyncHttpClient = new AsyncHttpClient();
// important: if not using pooled conn manager, must use local instance:
//        _blockingHttpClient = new DefaultHttpClient();
    }

    @Override public void start() { }

    @Override
    public void prepareForStop() {
        // nothing to do here...
    }

    @Override
    public void stop()
    {
        _closed.set(true);
    }
     
    // quick note: errors are in JSON, data as Smile.
        
    // And then the Real Thing, with basic JDK stuff:

    /**
     * Method called to request SYNCLIST from a local peer; this will include
     * a bit of piggyback information both ways, and may also lead to implicit
     * joining of caller into local cluster.
     */
    public SyncListResponse<?> fetchSyncList(ClusterViewByServerUpdatable cluster,
            TimeSpan timeout, NodeState remote, long lastClusterHash)
        throws InterruptedException
    {
        return _fetchSyncList(buildLocalSyncListUrl(cluster, remote, lastClusterHash), timeout,
                "Local fetchSyncList");
    }

    /**
     * Alternate accessor method used when calling remote nodes for information.
     * There is less implicit and/or piggybacked information to include and handle.
     */
    public SyncListResponse<?> fetchRemoteSyncList(NodeState localState, IpAndPort remoteEndpoint,
            long syncedUpTo, TimeSpan timeout)
        throws InterruptedException
    {
        return _fetchSyncList(buildRemoteSyncListUrl(localState, remoteEndpoint, syncedUpTo),
            timeout, "Remote fetchSyncList");
    }

    public SyncListResponse<?> _fetchSyncList(final String urlStr, TimeSpan timeout,
            String type)
        throws InterruptedException
    {
        HttpURLConnection conn;
        try {
            conn = prepareGet(urlStr, timeout);
            conn.setRequestProperty(HTTP_HEADER_ACCEPT, ACCEPTED_CONTENT_TYPES);
            conn.connect();
        } catch (Exception e) {
            LOG.warn("{} request to {} failed on send with Exception ({}): {}",
                    new Object[] { type, urlStr, e.getClass().getName(), e.getMessage()});
            return null;
        }
        
        // and if we are good, deal with status code, handle headers/response:
        try {
            int statusCode = conn.getResponseCode();
            if (IOUtil.isHTTPSuccess(statusCode)) {
                InputStream in = conn.getInputStream();
                SyncListResponse<?> resp;
                try {
                    resp = _syncListReader.readValue(in);
                } catch (IOException e) {
                    throw new IOException(type+" request returned by '"+urlStr+"', failed to parse Smile: "+e.getMessage());
                } finally {
                    try {
                        in.close();
                    } catch (Exception e) { }
                }
                // Bit of validation, as unknown props are allowed:
                if (resp.hasUnknownProperties()) {
                    LOG.warn("Unrecognized properties in SyncListResponse: "+resp.unknownProperties());
                }
                return resp;
            }
            handleHTTPFailure(conn, urlStr, statusCode, type);
        } catch (Exception e) {
            LOG.warn("{} request to {} failed on response with Exception ({}): {}",
                    new Object[] { type, urlStr, e.getClass().getName(), e.getMessage()});
        }            
        return null;
    }

    public InputStream readLocalSyncPullResponse(SyncPullRequest request, TimeSpan timeout,
            IpAndPort endpoint, AtomicInteger statusCodeWrapper,
            int expectedPayloadSize)
        throws IOException
    {
        return _readSyncPullResponse(request, timeout,
                _buildSyncPullUrl(endpoint),
                statusCodeWrapper, expectedPayloadSize);
    }

    public InputStream readRemoteSyncPullResponse(SyncPullRequest request, TimeSpan timeout,
            IpAndPort endpoint, AtomicInteger statusCodeWrapper,
            int expectedPayloadSize)
        throws IOException
    {
        return _readSyncPullResponse(request, timeout,
                _buildSyncPullUrl(endpoint),
                statusCodeWrapper, expectedPayloadSize);
    }
    
    protected InputStream _readSyncPullResponse(SyncPullRequest request, TimeSpan timeout,
            String endpointURL, AtomicInteger statusCodeWrapper,
            int expectedPayloadSize)
        throws IOException
    {
        byte[] reqPayload = _syncPullRequestWriter.writeValueAsBytes(request);
        final int reqLength = reqPayload.length;
        
        HttpURLConnection conn;
        OutputStream out = null;
        try {
            conn = preparePost(endpointURL, timeout, ContentType.JSON);
            // since we do know length in advance, let's just do this:
            conn.setFixedLengthStreamingMode(reqLength);
            conn.connect();
            out = conn.getOutputStream();
            out.write(reqPayload);
            out.close();
        } catch (Exception e) {
            LOG.warn("readSyncPullResponse request to {} failed on send with Exception ({}): {}",
                    new Object[] { endpointURL, e.getClass().getName(), e.getMessage()});
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
                    throw new IOException("readSyncPullResponse from '"+endpointURL+"' failed: "
                              +e.getMessage(), e);
                }
            }
            handleHTTPFailure(conn, endpointURL, statusCode,
                    "readSyncPullResponse (requesting "+request.size()+" entries (of about "+expectedPayloadSize+" mB total payload)"
                    );
        } catch (Exception e) {
            LOG.warn("syncList request to {} failed on response with Exception ({}): {}",
                    new Object[] { endpointURL, e.getClass().getName(), e.getMessage()});
        }            
        return null;
    }

    /**
     * Helper method used for sending simple status update message, usually
     * done when service starts up or shuts down.
     */
    public boolean sendStatusUpdate(ClusterViewByServerUpdatable cluster,
            TimeSpan timeout, IpAndPort remote, String newStatus)
    {
        final String urlStr = _buildNodeStatusUpdateUrl(cluster, remote, newStatus);
        HttpURLConnection conn;
        try {
            conn = preparePost(urlStr, timeout, ContentType.JSON);
            conn.setDoOutput(false);
            conn.connect();
        } catch (Exception e) {
            LOG.warn("sendStatusUpdate request to {} failed on send with Exception ({}): {}",
                    urlStr, e.getClass().getName(), e.getMessage());
            return false;
        }
        int statusCode;
        try {
            statusCode = conn.getResponseCode();
        } catch (IOException e) {
            LOG.warn("sendStatusUpdate request to {} failed with Exception ({}): {}",
                    urlStr, e.getClass().getName(), e.getMessage());
            return false;
        }
        if (IOUtil.isHTTPSuccess(statusCode)) {
            return true;
        }
        handleHTTPFailure(conn, urlStr, statusCode, "sendStatusUpdate");
        return false;
    }
    
    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */
    
    protected HttpURLConnection preparePost(String urlStr, TimeSpan timeout,
            ContentType contentType)
        throws IOException
    {
        return prepareHttpMethod(urlStr, timeout, "POST", true, contentType);
    }

    protected HttpURLConnection prepareGet(String urlStr, TimeSpan timeout)
            throws IOException
    {
        return prepareHttpMethod(urlStr, timeout, "GET", false, null);
    }
    
    protected HttpURLConnection prepareHttpMethod(String urlStr, TimeSpan timeout,
            String methodName, boolean sendInput, ContentType contentType)
        throws IOException
    {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(methodName);
        conn.setAllowUserInteraction(false);
        conn.setUseCaches(false);
        conn.setDoOutput(sendInput);
        conn.setDoInput(true); // we always read response
        if (contentType != null) { // should also indicate content type...
            conn.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType.toString());
        }
        
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
            LOG.warn("Problem reading ErrorStream for failed {} request to '{}': {}",
                    operation, urlStr, e.getMessage());
        }
        LOG.warn("Failed to process {} response from '{}': status code {}, response excerpt: {}",
                operation, urlStr, statusCode, msg);
    }

    protected String buildLocalSyncListUrl(ClusterViewByServerUpdatable cluster,
            NodeState remote, long lastClusterHash)
    {
        JdkHttpClientPathBuilder pathBuilder = new JdkHttpClientPathBuilder(remote.getAddress())
            .addPathSegments(_stuff.getServiceConfig().servicePathRoot);
        pathBuilder = _pathStrategy.appendSyncListPath(pathBuilder);
        pathBuilder = _buildSyncListUrl(pathBuilder, cluster.getLocalState(), remote.getSyncedUpTo());
        // this will include 'caller' param:
        pathBuilder = cluster.addClusterStateInfo(pathBuilder);
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_CLUSTER_HASH,
                String.valueOf(lastClusterHash));
        return pathBuilder.toString();
    }

    protected String buildRemoteSyncListUrl(NodeState localState, IpAndPort remoteEndpoint,
            long syncedUpTo)
    {
        JdkHttpClientPathBuilder pathBuilder = new JdkHttpClientPathBuilder(remoteEndpoint)
            .addPathSegments(_stuff.getServiceConfig().servicePathRoot);
        pathBuilder = _pathStrategy.appendRemoteSyncListPath(pathBuilder);
        pathBuilder = _buildSyncListUrl(pathBuilder, localState, syncedUpTo);

        // NOTE: no piggy-backing of local cluster info here
        return pathBuilder.toString();
    }
    
    protected JdkHttpClientPathBuilder _buildSyncListUrl(JdkHttpClientPathBuilder pathBuilder,
            NodeState local, final long syncedUpTo)
    {
        /* Need to be sure to pass the full range; remote end can do filtering,
         * (to reduce range if need be), but it needs to know full range
         * for initial auto-registration. Although ideally maybe we should
         * pass active and passive separately... has to do, for now.
         */
        final KeyRange syncRange = local.totalRange();
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_SINCE,
                String.valueOf(syncedUpTo));
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_KEYRANGE_START, String.valueOf(syncRange.getStart()));
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_KEYRANGE_LENGTH, String.valueOf(syncRange.getLength()));
        return pathBuilder;
    }
    
    protected String _buildNodeStatusUpdateUrl(ClusterViewByServerUpdatable cluster, IpAndPort remote,
            String state)
    {
        final KeyRange syncRange = cluster.getLocalState().totalRange();
        JdkHttpClientPathBuilder pathBuilder = new JdkHttpClientPathBuilder(remote)
            .addPathSegments(_stuff.getServiceConfig().servicePathRoot);
 
        pathBuilder = _pathStrategy.appendNodeStatusPath(pathBuilder);
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_KEYRANGE_START, String.valueOf(syncRange.getStart()));
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_KEYRANGE_LENGTH, String.valueOf(syncRange.getLength()));
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_TIMESTAMP,
                String.valueOf(_stuff.getTimeMaster().currentTimeMillis()));
        pathBuilder = pathBuilder.addParameter(QUERY_PARAM_STATE, state);
        // this will include 'caller' param:
        pathBuilder = cluster.addClusterStateInfo(pathBuilder);
        return pathBuilder.toString();
    }
    
    protected String _buildSyncPullUrl(IpAndPort endpoint)
    {
        final ServiceConfig config = _stuff.getServiceConfig();
        JdkHttpClientPathBuilder pathBuilder = new JdkHttpClientPathBuilder(endpoint)
            .addPathSegments(config.servicePathRoot);
        pathBuilder = _pathStrategy.appendSyncPullPath(pathBuilder);
        return pathBuilder.toString();
    }
}
