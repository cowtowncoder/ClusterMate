package com.fasterxml.clustermate.client.ahc;

import java.io.*;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.Loggable;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.cluster.ClusterServerNodeImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.Response;

/**
 * Intermediate base class used by accessors that use
 * Async HTTP Client library for HTTP Access.
 */
public abstract class AHCBasedAccessor<
    K extends EntryKey
>
    extends Loggable
{
    protected final AsyncHttpClient _httpClient;

    protected final ObjectMapper _mapper;

    protected final RequestPathStrategy<?> _pathFinder;
    
    protected EntryKeyConverter<K> _keyConverter;

    protected final ClusterServerNode _server;
    
    protected AHCBasedAccessor(StoreClientConfig<K,?> storeConfig, AsyncHttpClient hc,
            ClusterServerNode server)
    {
        super();
        _server = server;
        _httpClient = hc;
        _mapper = storeConfig.getJsonMapper();
        _pathFinder = storeConfig.getPathStrategy();
        _keyConverter = storeConfig.getKeyConverter();
    }

    /*
    /**********************************************************************
    /* Simple HTTP helper methods
    /**********************************************************************
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
        String str = resp.getHeader(ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH);
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
                    logWarn("Invalid '"+ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH+"h' header (from URL "+url+"): '"+str+"'");
                }
            }
        }
        return -1;
    }
    
    /*
    /**********************************************************************
    /* HTTP Response helpers
    /**********************************************************************
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
        String versionStr = headers.getFirstValue(ClusterMateConstants.CUSTOM_HTTP_HEADER_LAST_CLUSTER_UPDATE);
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

    protected ContentType findContentType(Response resp, ContentType defaultType)
    {
        String ctStr = resp.getContentType();
        if (ctStr != null) {
            ctStr = ctStr.trim();
            if (ctStr.length() > 0) {
                ContentType ct = ContentType.findType(ctStr);
                if (ct == null) {
                    logWarn("Unrecognized Content-Type ('"+ctStr+"'); defaulting to: "+defaultType);
                }
                return ct;
            }
        }
        return defaultType;
    }

    protected CallFailure failFromException(Exception e0, long startTime)
    {
        Throwable t = _unwrap(e0);
        if (t instanceof ConnectException) {
            return CallFailure.connectTimeout(_server, startTime, System.currentTimeMillis());
        }
        if (t instanceof SocketTimeoutException) {
            return CallFailure.timeout(_server, startTime, System.currentTimeMillis());
        }
        return CallFailure.clientInternal(_server, startTime, System.currentTimeMillis(), t);
    }
    
    /*
    /**********************************************************************
    /* Other
    /**********************************************************************
     */

    protected static Throwable _unwrap(Throwable t)
    {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    protected byte[] fromBase64(String b64str) {
        return _mapper.convertValue(b64str, byte[].class);
    }

    protected String toBase64(byte[] data) {
        return _mapper.convertValue(data, String.class);
    }
    
}
