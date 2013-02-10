package com.fasterxml.clustermate.client.jdk;

import java.io.*;
import java.net.HttpURLConnection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.shared.util.BufferRecycler;

import com.fasterxml.clustermate.api.*;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.cluster.ClusterServerNodeImpl;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

/**
 * Intermediate base class used by accessors that use
 * Async HTTP Client library for HTTP Access.
 */
public abstract class BaseJdkHttpAccessor<K extends EntryKey> extends Loggable
{
    /**
     * Not sure what is optimal chunk size, but 64k sounds like
     * a reasonable starting point.
     */
    protected final static int CHUNK_SIZE = 16 * 1024;
    
    /**
     * We can reuse read buffers as they are somewhat costly to
     * allocate, reallocate all the time.
     */
    final protected static BufferRecycler _bufferRecycler = new BufferRecycler(CHUNK_SIZE);

    protected final ObjectMapper _mapper;

    protected final RequestPathStrategy _pathFinder;

    protected EntryKeyConverter<K> _keyConverter;
    
    protected BaseJdkHttpAccessor(StoreClientConfig<K,?> storeConfig)
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

    protected long parseLongHeader(HttpURLConnection conn, String headerName)
    {
        String lenStr = conn.getHeaderField(headerName);
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

    protected void setTimeouts(HttpURLConnection conn, long timeoutMsecs)
    {
        // not optimal, will have to do:
        conn.setConnectTimeout((int) timeoutMsecs);
        conn.setReadTimeout((int) timeoutMsecs);
    }
    
    /*
    /**********************************************************************
    /* HTTP Request helpers
    /**********************************************************************
     */

    protected JdkHttpClientPathBuilder addChecksum(JdkHttpClientPathBuilder path, int checksum)
    {
        return path.addParameter(ClusterMateConstants.QUERY_PARAM_CHECKSUM,
                (checksum == 0) ? "0" : String.valueOf(checksum));
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
    protected void handleHeaders(ClusterServerNode server, HttpURLConnection conn,
            long requestTime)
    {
        String versionStr = conn.getHeaderField(ClusterMateConstants.CUSTOM_HTTP_HEADER_LAST_CLUSTER_UPDATE);
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

    protected ContentType findContentType(HttpURLConnection conn, ContentType defaultType)
    {
        String ctStr = conn.getContentType();
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
    /* HTTP content handling
    /**********************************************************************
     */

    protected long copy(InputStream in, OutputStream out, boolean closeInput) throws IOException
    {
        final BufferRecycler.Holder bufferHolder = _bufferRecycler.getHolder();        
        final byte[] copyBuffer = bufferHolder.borrowBuffer();
        
        try {
            long total = 0L;
            int count;
            while ((count = in.read(copyBuffer)) > 0) {
                out.write(copyBuffer, 0, count);
                total += count;
            }
            return total;
        } finally {
            bufferHolder.returnBuffer(copyBuffer);
            if (closeInput) {
                try { in.close(); } catch (IOException e0) {
                    logError("Failed to close input: "+e0.getMessage());
                }
            }
        }
    }
    
    protected void drain(HttpURLConnection conn, int statusCode)
    {
        try {
            if (statusCode >= 300) { // error
                drain(conn.getErrorStream());
            } else {
                drain(conn.getInputStream());
            }
        } catch (IOException e) {
            this.logWarn(e, "Problems closing response stream when draining");
        }
    }
    
    protected void drain(InputStream in)
    {
        try {
            while (in.skip(8000) > 0) { }
        } catch (IOException e) {
            this.logWarn(e, "Problems draining stream");
        }
        finally {
            try { in.close();
            } catch (IOException e2) {
                this.logWarn(e2, "Problems closing response stream after draining");
            }
        }
    }

    protected String getExcerpt(HttpURLConnection conn, int statusCode, int maxLen)
    {
        InputStream in = null;
        try {
            in = (statusCode >= 300) ? conn.getErrorStream() : conn.getInputStream();
            if (in == null) {
                return "[N/A]";
            }
            // NOTE: StoreMate has IOUtil, but won't take max length
            return getExcerpt(in, maxLen);
        } catch (Exception e) {
            return "[N/A due to error: "+e.getMessage()+"]";
        } finally {
            if (in != null) {
                try { in.close();
                } catch (IOException e2) {
                    this.logWarn(e2, "Problems closing response stream after draining");
                }
            }
        }
    }
    
    protected String getExcerpt(InputStream in, int maxLen) throws IOException
    {
        char[] buf = new char[maxLen];
        int offset = 0;
        InputStreamReader r = new InputStreamReader(in, "UTF-8");
        
        while (offset < buf.length) {
            int count = r.read(buf, offset, buf.length-offset);
            if (count <= 0) {
                break;
            }
            offset += count;
        }
        return new String(buf, 0, offset);
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
