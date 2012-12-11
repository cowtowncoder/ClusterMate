package com.fasterxml.clustermate.service.servlet;

import java.io.*;

import javax.servlet.http.*;

import com.fasterxml.clustermate.api.OperationType;
import com.fasterxml.clustermate.service.ServiceRequest;

/**
 * {@link ServiceRequest} implemented over native
 * Servlet request.
 */
public class ServletServiceRequest extends ServiceRequest
{
    /**
     * Underlying response object exposed by Servlet API.
     */
    protected final HttpServletRequest _request;

    protected InputStream _nativeStream;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public ServletServiceRequest(HttpServletRequest r, String path, boolean pathDecoded)
    {
        /* What exactly should we use here? getPathInfo() seems to decode
         * things, so it's not optimal; but getRequestURL() leaves
         * path...
         * 
         */
        super(path, pathDecoded, _resolveOperation(r.getMethod(), OperationType.CUSTOM));
        _request = r;
    }

    /*
    /**********************************************************************
    /* Basic ServiceRequest impl
    /**********************************************************************
     */

    @Override
    public InputStream getNativeInputStream() throws IOException {
        if (_nativeStream == null) {
            _nativeStream = _request.getInputStream();
        }
        return _nativeStream;
    }

    @Override
    public String getQueryParameter(String key) {
        return _request.getParameter(key);
    }

    @Override
    public String getHeader(String key)
    {
        return _request.getHeader(key);
    }
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    /**
     * Accessor for getting underlying {@link HttpServletRequest}
     */
    public HttpServletRequest getNativeRequest() {
        return _request;
    }
}
