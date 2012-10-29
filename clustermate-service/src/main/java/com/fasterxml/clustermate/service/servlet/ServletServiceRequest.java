package com.fasterxml.clustermate.service.servlet;

import java.io.*;

import javax.servlet.http.*;

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

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public ServletServiceRequest(HttpServletRequest r)
    {
        _request = r;
    }

    /*
    /**********************************************************************
    /* Basic ServiceRequest impl
    /**********************************************************************
     */

    @Override
    public InputStream getInputStream() throws IOException {
        return _request.getInputStream();
    }
    
    @Override
    public String getPath() {
        return _request.getPathInfo();
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

    public HttpServletRequest getNativeRequest() {
        return _request;
    }
}
