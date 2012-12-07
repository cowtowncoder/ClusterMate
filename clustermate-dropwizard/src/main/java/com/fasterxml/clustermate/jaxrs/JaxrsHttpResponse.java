package com.fasterxml.clustermate.jaxrs;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.service.ServiceResponse;

public class JaxrsHttpResponse extends ServiceResponse
{
    protected ResponseBuilder _builder;

    protected int _statusCode;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public JaxrsHttpResponse() {
        _statusCode = 200;
    }

    public Response buildResponse()
    {
        if (_builder == null) {
            _builder = Response.status(_statusCode);
        }
        // And what kind of content are we to return, if anything?
        if (_streamingContent != null) {
            long len = _streamingContent.getLength();
            if (len >= 0L) {
                this.setContentLength(len);
            }
            _builder = _builder.entity(new StreamingContentAsStreamingOutput(_streamingContent));
        } else if (_entity != null) {
            _builder = _builder.entity(_entity);
        }
        return _builder.build();
    }
    
    /*
    /**********************************************************************
    /* Basic VHttpResponse impl
    /**********************************************************************
     */

    @Override
    public long getBytesWritten() {
        // TODO: implement... how?
        return 0L;
    }
    
    @Override
    public int getStatus() {
        return _statusCode;
    }
    
    @Override
    public JaxrsHttpResponse set(int code, Object entity)
    {
        _statusCode = code;
        return setEntity(entity);
    }
    
    @Override
    public JaxrsHttpResponse setStatus(int code)
    {
        _statusCode = code;
        return this;
    }

    @Override
    public JaxrsHttpResponse addHeader(String key, String value)
    {
        if (_builder == null) {
            _builder = Response.status(_statusCode);
        }
        _builder = _builder.header(key, value);
        return this;
    }

    @Override
    public JaxrsHttpResponse addHeader(String key, int value)
    {
        if (_builder == null) {
            _builder = Response.status(_statusCode);
        }
        _builder = _builder.header(key, value);
        return this;
    }

    @Override
    public JaxrsHttpResponse addHeader(String key, long value)
    {
        if (_builder == null) {
            _builder = Response.status(_statusCode);
        }
        _builder = _builder.header(key, value);
        return this;
    }
    
    @Override
    public JaxrsHttpResponse setContentType(String contentType) {
        if (_builder == null) {
            _builder = Response.status(_statusCode);
        }
        _builder = _builder.type(contentType);
        return this;
    }

    @Override
    public JaxrsHttpResponse setContentLength(long length) {
        return addHeader(ClusterMateConstants.HTTP_HEADER_CONTENT_LENGTH, length);
    }
}
