package com.fasterxml.clustermate.std;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.IOUtil;

/**
 * Implementation of {@link ClusterStatusAccessor} that uses JDK
 * HTTP access functionality.
 */
public class JdkClusterStatusAccessor extends ClusterStatusAccessor
{
    protected final ClusterStatusAccessor.Converter _converter;

    protected final String[] _basePath;
    
    protected final RequestPathStrategy<PathType> _paths;

    @SuppressWarnings("unchecked")
    public JdkClusterStatusAccessor(ClusterStatusAccessor.Converter converter,
            String[] basePath,
            RequestPathStrategy<?> paths)
    {
        _converter = converter;
        _basePath = basePath;
        _paths = (RequestPathStrategy<PathType>) paths;
    }

    @Override
    public ClusterStatusMessage getClusterStatus(IpAndPort ip, long timeoutMsecs)
        throws IOException
    {
        JdkHttpClientPathBuilder pathBuilder = new JdkHttpClientPathBuilder(ip)
            .addPathSegments(_basePath);
        pathBuilder = _paths.appendPath(pathBuilder, PathType.NODE_STATUS);
        return getClusterStatus(pathBuilder.toString(), timeoutMsecs);
    }

    @Override
    public ClusterStatusMessage getClusterStatus(String endpoint, long timeoutMsecs)
        throws IOException
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        if (timeoutMsecs < MIN_TIMEOUT_MSECS) {
            return null;
        }
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
        ClusterStatusMessage result;
        try {
            result = _converter.fromJSON(in);
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
