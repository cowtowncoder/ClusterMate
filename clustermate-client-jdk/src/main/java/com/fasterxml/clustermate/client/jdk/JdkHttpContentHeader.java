package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.CallConfig;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.ContentHeader;
import com.fasterxml.clustermate.client.call.ReadCallParameters;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;
import com.fasterxml.storemate.shared.util.IOUtil;

/**
 * Helper object for making HEAD requests.
 */
public class JdkHttpContentHeader<K extends EntryKey>
    extends BaseJdkHttpAccessor<K>
    implements ContentHeader<K>
{
    public JdkHttpContentHeader(StoreClientConfig<K,?> storeConfig,
            ClusterServerNode server)
    {
        super(storeConfig, server);
    }

    /*
    /**********************************************************************
    /* Call implementation
    /**********************************************************************
     */
    
    @Override
    public JdkHttpHeadCallResult tryHead(CallConfig config, ReadCallParameters params,
            long endOfTime, K contentId)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        long timeoutMsecs = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeoutMsecs < config.getMinimumTimeoutMsecs()) {
            return new JdkHttpHeadCallResult(CallFailure.timeout(_server, startTime, startTime));
        }
        try {
            JdkHttpClientPathBuilder path = _server.rootPath();
            path = _pathFinder.appendStoreEntryPath(path);
            path = _keyConverter.appendToPath(path, contentId);
            if (params != null) {
                path = params.appendToPath(path, contentId);
            }
            URL url = path.asURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            int statusCode = sendRequest("HEAD", conn, path, timeoutMsecs);
            
            // one thing first: handle standard headers, if any?
            handleHeaders(_server, conn, startTime);

            // call ok?
            if (!IOUtil.isHTTPSuccess(statusCode)) {
                // if not, why not? Any well-known problems? (besides timeout that was handled earlier)
                return new JdkHttpHeadCallResult(CallFailure.general(_server, statusCode, startTime,
                		System.currentTimeMillis(), "N/A"));
            }
            try {
                return new JdkHttpHeadCallResult(conn, _server,ClusterMateConstants.HTTP_STATUS_OK,
                        parseLongHeader(conn, ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH));
            } catch (Exception e) {
                return new JdkHttpHeadCallResult(CallFailure.formatException(_server,
                        statusCode, startTime, System.currentTimeMillis(), e.getMessage()));
            }
        } catch (Exception e) {
            return new JdkHttpHeadCallResult(failFromException(e, startTime));
        }
    }
}
