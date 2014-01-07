package com.fasterxml.clustermate.client.ahc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ning.http.client.*;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.CallConfig;
import com.fasterxml.clustermate.client.call.ContentHeader;
import com.fasterxml.clustermate.client.call.ReadCallParameters;
import com.fasterxml.storemate.shared.util.IOUtil;

/**
 * Helper object for making HEAD requests.
 */
public class AHCContentHeader<K extends EntryKey, P extends Enum<P>>
    extends AHCBasedAccessor<K,P>
    implements ContentHeader<K>
{
    protected final ClusterServerNode _server;
    
    public AHCContentHeader(StoreClientConfig<K,?> storeConfig, P endpoint,
            AsyncHttpClient hc, ClusterServerNode server)
    {
        super(storeConfig, endpoint, hc);
        _server = server;
    }

    /*
    /**********************************************************************
    /* Call implementation
    /**********************************************************************
     */
    
    @Override
    public AHCHeadCallResult tryHead(CallConfig config, ReadCallParameters params,
            long endOfTime, K contentId)
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        final long startTime = System.currentTimeMillis();
        long timeout = Math.min(endOfTime - startTime, config.getGetCallTimeoutMsecs());
        if (timeout < config.getMinimumTimeoutMsecs()) {
            return new AHCHeadCallResult(CallFailure.timeout(_server, startTime, startTime));
        }

        try {
            AHCPathBuilder path = _server.rootPath();
            path = _pathFinder.appendPath(path, _endpoint);
            path = _keyConverter.appendToPath(path, contentId);
            if (params != null) {
                path = params.appendToPath(path, contentId);
            }
            BoundRequestBuilder reqBuilder = path.headRequest(_httpClient);

            HeadHandler<K,P> hh = new HeadHandler<K,P>(this, _server, startTime);
            ListenableFuture<Object> futurama = _httpClient.executeRequest(reqBuilder.build(), hh);
            // First, see if we can get the answer without time out...
            try {
                futurama.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return new AHCHeadCallResult(CallFailure.timeout(_server, startTime, System.currentTimeMillis()));
            }
            // and if so, is it successful?
            int statusCode = hh.statusCode;
            // call ok?
            if (!IOUtil.isHTTPSuccess(statusCode)) {
                if (hh.fail != null) {
                    return new AHCHeadCallResult(CallFailure.clientInternal(_server, startTime, System.currentTimeMillis(), hh.fail));
                }
                // if not, why not? Any well-known problems? (besides timeout that was handled earlier)
                return new AHCHeadCallResult(CallFailure.general(_server, statusCode, startTime,
                		System.currentTimeMillis(), "N/A"));
            }
            String lenStr = hh.contentLength;
            try {
                long l;
                if (lenStr == null || (lenStr = lenStr.trim()).length() == 0) {
                    l = -1;
                } else {
            		l = Long.parseLong(lenStr.trim());
                }
                return new AHCHeadCallResult(200, l);
            } catch (Exception e) {
                String desc = (lenStr == null) ? "null" : "\""+lenStr+"\"";
                return new AHCHeadCallResult(CallFailure.formatException(_server,
                        statusCode, startTime, System.currentTimeMillis(),
                        "Invalid '"+ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH+"' value: "+desc));
            }
        } catch (Exception e) {
            return new AHCHeadCallResult(CallFailure.clientInternal(_server, startTime, System.currentTimeMillis(), e));
        }
    }

    private final static class HeadHandler<K extends EntryKey, P extends Enum<P>>
        implements AsyncHandler<Object>
    {
        private final AHCContentHeader<K,P> _parent;
        private final ClusterServerNode _server;
        private final long _startTime;
    	
        public Throwable fail;
        public int statusCode = -1;
        public String contentLength = null;

        public HeadHandler(AHCContentHeader<K,P> parent, ClusterServerNode server, long startTime)
        {
            _parent = parent;
            _server = server;
            _startTime = startTime;
        }
    	
        @Override
        public void onThrowable(Throwable t) {
            fail = t;
        }

        @Override
        public com.ning.http.client.AsyncHandler.STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) {
            // we shouldn't get any, but whatever
            return STATE.CONTINUE;
        }

        @Override
        public com.ning.http.client.AsyncHandler.STATE onStatusReceived(HttpResponseStatus responseStatus) {
            statusCode = responseStatus.getStatusCode();
            if (!IOUtil.isHTTPSuccess(statusCode)) {
                return STATE.ABORT;
            }
            return STATE.CONTINUE;
        }

        @Override
        public com.ning.http.client.AsyncHandler.STATE onHeadersReceived(HttpResponseHeaders headers)
        {
            _parent.handleHeaders(_server, headers.getHeaders(), _startTime);
            contentLength = headers.getHeaders().getFirstValue(ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH);
            return STATE.CONTINUE;
        }

        @Override
        public Object onCompleted() {
            return null;
        }
    }
}
