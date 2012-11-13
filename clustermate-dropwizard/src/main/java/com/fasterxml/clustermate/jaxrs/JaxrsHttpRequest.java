package com.fasterxml.clustermate.jaxrs;

import java.io.*;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.clustermate.api.OperationType;
import com.fasterxml.clustermate.service.ServiceRequest;

public class JaxrsHttpRequest extends ServiceRequest
{
    protected final UriInfo _uriInfo;
    
    protected final HttpHeaders _headers;

    protected MultivaluedMap<String, String> _headerValues;
    
    protected MultivaluedMap<String, String> _qp;

    public JaxrsHttpRequest(UriInfo uriInfo, HttpHeaders headers, String decodedPath,
            String operationType)
    {
        this(uriInfo, headers, decodedPath, _resolveOperation(operationType, OperationType.CUSTOM));
    }
    
    public JaxrsHttpRequest(UriInfo uriInfo, HttpHeaders headers, String decodedPath,
            OperationType operationType)
    {
        super(decodedPath, true, operationType);
        _uriInfo = uriInfo;
        _headers = headers;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        // must be directly assigned...
        throw new IOException("No InputStream available via "+getClass().getName()+" instance");
    }

    @Override
    public String getQueryParameter(String key) {
        if (_qp == null) {
            _qp = _uriInfo.getQueryParameters();
            if (_qp == null) {
                return null;
            }
        }
        return _qp.getFirst(key);
    }

    @Override
    public String getHeader(String key) {
        if (_headerValues == null) {
            _headerValues = (_headers == null)? null : _headers.getRequestHeaders();
            if (_headerValues == null) {
                return null;
            }
        }
        return _headerValues.getFirst(key);
    }
}
