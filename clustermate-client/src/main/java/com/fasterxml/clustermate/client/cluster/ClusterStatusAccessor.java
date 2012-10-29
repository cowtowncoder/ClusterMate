package com.fasterxml.clustermate.client.cluster;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.clustermate.api.ClusterStatusResponse;
import com.fasterxml.clustermate.client.Loggable;
import com.fasterxml.clustermate.client.NetworkClient;
import com.fasterxml.clustermate.client.impl.StoreClientConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.RequestPathBuilder;
import com.fasterxml.storemate.shared.util.IOUtil;

/**
 * Helper class that handles details of getting cluster status information
 * from a store node.
 */
public class ClusterStatusAccessor
    extends Loggable
{
    protected final static long MIN_TIMEOUT_MSECS = 10L;
    
    protected final StoreClientConfig<?,?> _clientConfig;

    protected final NetworkClient<?> _networkClient;

    protected final ObjectMapper _mapper;
    
    public ClusterStatusAccessor(StoreClientConfig<?,?> clientConfig,
            NetworkClient<?> networkClient)
    {
        super(ClusterStatusAccessor.class);
        _clientConfig = clientConfig;
        _networkClient = networkClient;
        _mapper = clientConfig.getJsonMapper();
    }

    public ClusterStatusResponse getClusterStatus(IpAndPort ip, long timeoutMsecs)
        throws IOException
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        if (timeoutMsecs < MIN_TIMEOUT_MSECS) {
            return null;
        }

        RequestPathBuilder pathBuilder = _networkClient.pathBuilder(ip);
        for (String part : _clientConfig.getBasePath()) {
            pathBuilder = pathBuilder.addPathSegment(part);
        }
        pathBuilder = _clientConfig.getPathStrategy().appendNodeStatusPath(pathBuilder);
        String endpoint = pathBuilder.toString();

System.err.println("END POINT: "+endpoint);
        
        HttpURLConnection conn;

        try {
            URL url = new URL(endpoint);
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            throw new IOException("Can not access Cluster state using '"+endpoint+"': "+e.getMessage());
        }
        conn.setRequestMethod("GET");
        // not optimal, will have to do:
        conn.setConnectTimeout((int) timeoutMsecs);
        conn.setReadTimeout((int) timeoutMsecs);
        int status = conn.getResponseCode();
        if (!IOUtil.isHTTPSuccess(status)) {
            // should we read the error message?
            throw new IOException("Failed to access Cluster state using '"+endpoint+"': response code "
                    +status);
        }
        
        InputStream in;
        try {
            in = conn.getInputStream();
        } catch (IOException e) {
            throw new IOException("Can not access Cluster state using '"+endpoint+"': "+e.getMessage());
        }
        ClusterStatusResponse result;
        try {
            result = _mapper.readValue(in, ClusterStatusResponse.class);
        } catch (IOException e) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', failed to parse JSON: "+e.getMessage());
        } finally {
            try {
                in.close();
            } catch (IOException e) { }
        }
        // validate briefly, just in case:
        if (result.local == null) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', missing 'local' info");
        }
        if (result.local.getAddress() == null) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', missing 'local.address' info");
        }
        if (result.remote == null) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', missing 'remote' info"); 
        }
        return result;
    }
}
