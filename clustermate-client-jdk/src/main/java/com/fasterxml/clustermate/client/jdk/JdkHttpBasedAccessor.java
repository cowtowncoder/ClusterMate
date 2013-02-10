package com.fasterxml.clustermate.client.jdk;

import java.io.*;
import java.net.HttpURLConnection;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.client.CallFailure;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.Loggable;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.cluster.ClusterServerNodeImpl;

/**
 * Intermediate base class used by accessors that use
 * Async HTTP Client library for HTTP Access.
 */
public abstract class JdkHttpBasedAccessor<K extends EntryKey> extends Loggable
{
    protected final ObjectMapper _mapper;

    protected final RequestPathStrategy _pathFinder;

    protected EntryKeyConverter<K> _keyConverter;
    
    protected JdkHttpBasedAccessor(StoreClientConfig<K,?> storeConfig)
    {
        super();
        _mapper = storeConfig.getJsonMapper();
        _pathFinder = storeConfig.getPathStrategy();
        _keyConverter = storeConfig.getKeyConverter();
    }

    /*
    /**********************************************************************
    /* Simple HTTP helper methods
    /**********************************************************************
     */

    protected long _parseLongHeader(HttpURLConnection conn, String headerName)
    {
        String lenStr = conn.getHeaderField(headerName);
       long l;
       if (lenStr == null) {
           return -1L;
       }
       lenStr = lenStr.trim();
       if (lenStr.length() == 0) {
           return -1L;
       }
       try {
           return  Long.parseLong(lenStr.trim());
       } catch (NumberFormatException e) {
           String desc = (lenStr == null) ? "null" : "\""+lenStr+"\"";
           throw new IllegalArgumentException("Bad numeric value for header '"+headerName+"': "+desc);
       }
    }
    
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
    /* HTTP Request helpers
    /**********************************************************************
     */

    protected BoundRequestBuilder addCheckSum(BoundRequestBuilder reqBuilder, int checksum)
    {
        reqBuilder = reqBuilder.addQueryParameter(ClusterMateConstants.QUERY_PARAM_CHECKSUM,
                (checksum == 0) ? "0" : String.valueOf(checksum));
        return reqBuilder;
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
