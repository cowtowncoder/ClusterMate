package com.fasterxml.clustermate.service;

import java.io.*;

import com.fasterxml.storemate.shared.ByteRange;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.DecodableRequestPath;

/**
 * Interface class that defines abstraction implemented by classes that
 * enclose details of a (HTTP) request that server receives.
 *<p>
 * Separated out to allow handlers to operate independent of the
 * container like Servlet or JAX-RS container.
 */
public abstract class ServiceRequest
    implements DecodableRequestPath
{
    /**
     * This is the full non-decoded original path of the request.
     */
    protected final String _originalFullPath;

    /**
     * Path override assigned by {@link #setPath}, if any.
     */
    protected String _currentPath;

    /**
     * Whether path we have has been URL decoded or not.
     */
    protected final boolean _isPathDecoded;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected ServiceRequest(String origPath, boolean isPathDecoded)
    {
        _originalFullPath = origPath;
        _currentPath = origPath;
        _isPathDecoded = isPathDecoded;
    }
    
    /*
    /**********************************************************************
    /* Access to information other than path
    /**********************************************************************
     */

    public abstract InputStream getInputStream() throws IOException;
    
    public ByteRange findByteRange()
    {
        String rangeStr = getHeader(ClusterMateConstants.HTTP_HEADER_RANGE_FOR_REQUEST);
        return (rangeStr == null) ? null : ByteRange.valueOf(rangeStr);
    }

    /*
    /**********************************************************************
    /* Path handling (mostly from DecodableRequestPath)
    /**********************************************************************
     */

    @Override
    public abstract String getQueryParameter(String key);

    @Override
    public abstract String getHeader(String key);
    
    @Override
    public String getPath() {
        return _currentPath;
    }

    @Override
    public String getDecodedPath() {
        if (_isPathDecoded) {
            return _currentPath;
        }
        if (_currentPath == null) {
            return null;
        }
        return _decodePath(_currentPath);
    }

    @Override
    public boolean isPathDecoded() { return _isPathDecoded; }
    
    
    @Override
    public void setPath(String path) {
        _currentPath = path;
    }
    
    @Override
    public String nextPathSegment() {
        String str = _currentPath;
        if (str == null) {
            return null;
        }
        int ix = str.indexOf('/');
        if (ix < 0) { // last segment...
            _currentPath = null;
            return str;
        }
        _currentPath = str.substring(ix+1);
        str = str.substring(0, ix);
        return str;
    }

    @Override
    public boolean matchPathSegment(String segment) {
        String str = _currentPath;
        final int len = segment.length();
        if (str == null || !str.startsWith(segment)) {
            return false;
        }
        // ok; we now it starts with it, but is it followed by a slash?
        if (str.length() == len) { // full match
            _currentPath = null;
            return true;
        }
        if (str.charAt(len) == '/') { // yeppers
            _currentPath = _currentPath.substring(len+1);
            return true;
        }
        return false;
    }

    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    protected String _decodePath(String encodedPath)
    {
        int ix = encodedPath.indexOf('%');
        if (ix < 0) {
            return encodedPath;
        }
//        throw new UnsupportedOperationException("Bad path: "+encodedPath);
        return encodedPath;
    }

}
