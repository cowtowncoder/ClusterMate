package com.fasterxml.clustermate.service;

import java.io.*;

import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.HTTPConstants;

/**
 * Interface class that defines abstraction implemented by classes that
 * enclose details of a (HTTP) request that server receives.
 *<p>
 * Separated out to allow handlers to operate independent of the
 * container like Servlet or JAX-RS container.
 */
public abstract class ServiceRequest
{
    public abstract String getPath();
    
    public abstract String getQueryParameter(String key);

    public abstract String getHeader(String key);

    public abstract InputStream getInputStream() throws IOException;
    
    public ByteRange findByteRange()
    {
        String rangeStr = getHeader(HTTPConstants.HTTP_HEADER_RANGE_FOR_REQUEST);
        return (rangeStr == null) ? null : ByteRange.valueOf(rangeStr);
    }
}
