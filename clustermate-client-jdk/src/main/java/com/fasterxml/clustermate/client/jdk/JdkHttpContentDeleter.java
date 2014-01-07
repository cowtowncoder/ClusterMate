package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.CallFailure;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.CallConfig;
import com.fasterxml.clustermate.client.call.ContentDeleter;
import com.fasterxml.clustermate.client.call.DeleteCallParameters;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public class JdkHttpContentDeleter<K extends EntryKey,P extends Enum<P>>
    extends BaseJdkHttpAccessor<K,P>
    implements ContentDeleter<K>
{
    protected final ClusterServerNode _server;

    public JdkHttpContentDeleter(StoreClientConfig<K,?> storeConfig, P endpoint,
            ClusterServerNode server)
    {
        super(storeConfig, endpoint);
        _server = server;
    }

    @Override
    public CallFailure tryDelete(CallConfig config, DeleteCallParameters params,
            long endOfTime, K contentId)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        final long timeoutMsecs = Math.min(endOfTime - startTime, config.getDeleteCallTimeoutMsecs());
        if (timeoutMsecs < config.getMinimumTimeoutMsecs()) {
            return CallFailure.timeout(_server, startTime, startTime);
        }
        try {
            JdkHttpClientPathBuilder path = _server.rootPath();
            path = _pathFinder.appendPath(path, _endpoint);
            path = _keyConverter.appendToPath(path, contentId);
            if (params != null) {
                path = params.appendToPath(path, contentId);
            }
            URL url = path.asURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            int statusCode = sendRequest("DELETE", conn, path, timeoutMsecs);
            
            // one thing first: handle standard headers, if any?
            handleHeaders(_server, conn, startTime);

            // call ok?
            if (!IOUtil.isHTTPSuccess(statusCode)) {
                // if not, why not? Any well-known problems? (besides timeout that was handled earlier)

                // then the default fallback
                String msg = getExcerpt(conn, statusCode, config.getMaxExcerptLength());
                return CallFailure.general(_server, statusCode, startTime, System.currentTimeMillis(), msg);
            }
            drain(conn, statusCode);
            return null;
        } catch (Exception e) {
            return CallFailure.clientInternal(_server, startTime, System.currentTimeMillis(), e);
        }
    }
}
