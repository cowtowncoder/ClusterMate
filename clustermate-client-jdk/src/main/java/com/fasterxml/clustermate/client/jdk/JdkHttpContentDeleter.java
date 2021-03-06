package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.CallConfig;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.ContentDeleter;
import com.fasterxml.clustermate.client.call.DeleteCallParameters;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public class JdkHttpContentDeleter<K extends EntryKey>
    extends BaseJdkHttpAccessor<K>
    implements ContentDeleter<K>
{
    public JdkHttpContentDeleter(StoreClientConfig<K,?> storeConfig,
            ClusterServerNode server)
    {
        super(storeConfig, server);
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
            path = _pathFinder.appendStoreEntryPath(path);
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
            return failFromException(e, startTime);
        }
    }
}
