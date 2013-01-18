package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.*;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * {@link ServiceResponse} implemented over native
 * Servlet response.
 */
public class ServletServiceResponse extends ServiceResponse
{
    /**
     * Underlying response object exposed by Servlet API.
     */
    protected final HttpServletResponse _response;

    /**
     * Since 'getStatus()' was only added in Servlet 3.0, let's actually
     * keep track of it here.
     */
    protected int _statusCode;
    
    protected long _responseLength;

    protected OutputStream _output;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public ServletServiceResponse(HttpServletResponse r)
    {
        _response = r;
    }

    public void writeOut(ObjectWriter writer) throws IOException
    {
        if (_output == null) {
            _output = _response.getOutputStream();
        }
        if (_streamingContent != null) {
            long len = _streamingContent.getLength();
            if (len >= 0L) {
                setContentLength(len);
            }
            _streamingContent.writeContent(_output);
        } else if (_entity != null) {
            writer.writeValue(_output, _entity);
        }
    }
    
    /*
    /**********************************************************************
    /* Basic VHttpResponse impl
    /**********************************************************************
     */

    @Override
    public long getBytesWritten() {
        return _responseLength;
    }
    
    @Override
    public int getStatus() {
        return _statusCode;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ServletServiceResponse set(int code, Object entity)
    {
        return setStatus(code).setEntity(entity);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ServletServiceResponse setStatus(int code)
    {
        _statusCode = code;
        _response.setStatus(code);
        return this;
    }

    @Override
    public ServletServiceResponse setContentType(String contentType) {
        _response.setContentType(contentType);
        return this;
    }
 
    @Override
    public ServletServiceResponse setContentLength(long length)
    {
        _responseLength = length;        
        // not sure if this would work but:
        if (length > Integer.MAX_VALUE) {
            return addHeader(ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH, length);
        }
        _response.setContentLength((int) length);
        return this;
    }
    
    @Override
    public ServletServiceResponse addHeader(String key, String value)
    {
        _response.addHeader(key, value);
        return this;
    }

    @Override
    public ServletServiceResponse addHeader(String key, int value)
    {
        _response.addIntHeader(key, value);
        return this;
    }

    @Override
    public ServletServiceResponse addHeader(String key, long value)
    {
        _response.addHeader(key, String.valueOf(value));
        return this;
    }
}
