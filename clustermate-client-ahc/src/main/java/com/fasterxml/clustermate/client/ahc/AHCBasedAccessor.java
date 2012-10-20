package com.fasterxml.clustermate.client.ahc;

import java.io.*;

import com.fasterxml.clustermate.client.cluster.ClusterServerNode;
import com.fasterxml.clustermate.client.cluster.ClusterServerNodeImpl;
import com.fasterxml.clustermate.client.cluster.Loggable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.api.HTTPConstants;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.Response;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

/**
 * Intermediate base class used by accessors that use
 * Async HTTP Client library for HTTP Access.
 */
public abstract class AHCBasedAccessor extends Loggable
{
    protected final AsyncHttpClient _httpClient;

    protected final ObjectMapper _mapper;

    
//    protected final int _excerptLength;
    
    protected AHCBasedAccessor(AsyncHttpClient hc, ObjectMapper m)
    {
        super();
        _httpClient = hc;
        _mapper = m;
//        _excerptLength = config.getExcerptLength();
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Simple HTTP helper methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected void drain(Response resp)
    {
        try {
            InputStream in = resp.getResponseBodyAsStream();
            while (in.skip(8000) > 0) { }
            in.close();
        } catch (IOException e) { }
    }

    protected String getExcerpt(Response resp, int maxLen)
    {
        try {
            return resp.getResponseBodyExcerpt(maxLen);
        } catch (Exception e) {
            return "[N/A due to error: "+e.getMessage()+"]";
        }
    }

    protected int _getContentLength(Response resp)
    {
        String str = resp.getHeader(HTTPConstants.HTTP_HEADER_CONTENT_LENGTH);
        if (str != null) {
            str = str.trim();
            if (str.length() > 0 && Character.isDigit(str.charAt(0))) {
                try {
                    return Integer.parseInt(str);
                } catch (Exception e) {
                    String url = "N/A";
                    try {
                        url = String.valueOf(resp.getUri());
                    } catch (Exception e2) { }
                    logWarn("Invalid '"+HTTPConstants.HTTP_HEADER_CONTENT_LENGTH+"h' header (from URL "+url+"): '"+str+"'");
                }
            }
        }
        return -1;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // HTTP Request helpers
    ///////////////////////////////////////////////////////////////////////
     */

    /*
    protected BoundRequestBuilder addStandardParams(BoundRequestBuilder reqBuilder,
            ClientId clientId)
    {
        reqBuilder = reqBuilder.addQueryParameter(HTTPConstants.HTTP_QUERY_PARAM_CLIENT_ID,
                String.valueOf(clientId));
        return reqBuilder;
    }
    */

    protected BoundRequestBuilder addCheckSum(BoundRequestBuilder reqBuilder, int checksum)
    {
        reqBuilder = reqBuilder.addQueryParameter(HTTPConstants.HTTP_QUERY_PARAM_CHECKSUM,
                (checksum == 0) ? "0" : String.valueOf(checksum));
        return reqBuilder;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // HTTP Response helpers
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Helper method that takes care of processing state based on any
     * standard headers we might pick up
     */
    protected void handleHeaders(ClusterServerNode server, Response resp,
            long requestTime)
    {
        handleHeaders(server, resp.getHeaders(), requestTime);
    }

    protected void handleHeaders(ClusterServerNode server, FluentCaseInsensitiveStringsMap headers,
            long requestTime)
    {
        if (headers == null) {
            return;
        }
        String versionStr = headers.getFirstValue(HTTPConstants.CUSTOM_HTTP_HEADER_LAST_CLUSTER_UPDATE);
        if (versionStr != null && (versionStr = versionStr.trim()).length() > 0) {
            try {
                long l = Long.parseLong(versionStr);
                long responseTime = System.currentTimeMillis();
                ((ClusterServerNodeImpl) server).updateLastClusterUpdateAvailable(l, requestTime, responseTime);
            } catch (Exception e) {
                logWarn("Invalid Cluster version String '"+versionStr+"' received from "
                        +server.getAddress());
            }
        }
    }
}
