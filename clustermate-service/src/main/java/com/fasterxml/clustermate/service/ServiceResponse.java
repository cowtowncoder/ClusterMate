package com.fasterxml.clustermate.service;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.msg.StreamingResponseContent;

/**
 * Interface class that defines interface of (HTTP) Responses
 * server returns to caller.
 *<p>
 * Separated out to allow handlers to operate independent of the
 * container like Servlet or JAX-RS container.
 */
public abstract class ServiceResponse
{
    /**
     * Raw entity to serialize and return, if any.
     *<p>
     * Note that either this OR <code>_streamingContent</code> can be non-null.
     */
    protected Object _entity;

    /**
     * Content to write out, if any.
     *<p>
     * Note that either this OR <code>_entity</code> can be non-null.
     */
    protected StreamingResponseContent _streamingContent;

    /*
    /**********************************************************************
    /* Statistics
    /**********************************************************************
     */

    /**
     * Accessor for number of bytes written through this response.
     */
    public abstract long getBytesWritten();
    
    /*
    /**********************************************************************
    /* Low-level response building
    /**********************************************************************
     */
    
    public abstract <RESP extends ServiceResponse> RESP  set(int code, Object entity);

    public abstract <RESP extends ServiceResponse> RESP  setStatus(int code);
    
    public abstract ServiceResponse addHeader(String key, String value);

    public abstract ServiceResponse addHeader(String key, int value);

    public abstract ServiceResponse addHeader(String key, long value);
    
    /**
     * Method for specifying POJO to serialize as content of response;
     * either as streaming content (if entity is of type
     * {@link StreamingResponseContent}); or as something to serialize
     * using default serialization mechanism (usually JSON).
     */
    @SuppressWarnings("unchecked")
    public final <RESP extends ServiceResponse> RESP setEntity(Object e)
    {
        if (e instanceof StreamingResponseContent) {
            _entity = null;
            _streamingContent = (StreamingResponseContent) e;
        } else {
            _entity = e;
            _streamingContent = null;
        }
        return (RESP) this;
    }
    
    /*
    /**********************************************************************
    /* Basic accessors
    /**********************************************************************
     */

    public abstract int getStatus();

    public final boolean isError() { return getStatus() >= 300; }

    public final boolean hasEntity() { return _entity != null; }
    public final boolean hasStreamingContent() { return _streamingContent != null; }

    @SuppressWarnings("unchecked")
    public <T> T getEntity() {
        return (T) _entity;
    }
    
    /*
    /**********************************************************************
    /* High(er)-level response building; semantic headers
    /**********************************************************************
     */

    public abstract ServiceResponse setContentType(String contentType);

    public ServiceResponse setContentTypeJson() {
        return setContentType("application/json");
    }

    public ServiceResponse setContentTypeText() {
        return setContentType("text/plain");
    }
    
    public final ServiceResponse setBodyCompression(String type) {
        return addHeader(ClusterMateConstants.HTTP_HEADER_COMPRESSION, type);
    }
    
    public abstract ServiceResponse setContentLength(long length);

    /*
    /**********************************************************************
    /* High(er)-level response building; ok cases
    /**********************************************************************
     */
    
    public final ServiceResponse ok() {
        return setStatus(200);
    }
    
    public final ServiceResponse ok(Object entity) {
        return setEntity(entity);
    }

    public final ServiceResponse noContent() {
        return setStatus(204);
    }

    public final ServiceResponse partialContent(Object entity, String rangeDesc) {
        // 206 means "partial content"
        return set(ClusterMateConstants.HTTP_STATUS_OK_PARTIAL, entity)
                .addHeader(ClusterMateConstants.HTTP_HEADER_RANGE_FOR_RESPONSE, rangeDesc);
    }

    /*
    /**********************************************************************
    /* High(er)-level response building; error cases
    /**********************************************************************
     */

    public final <RESP extends ServiceResponse> RESP badMethod() {
        // Method Not Allowed
        return setStatus(405);
    }
    
    public final <RESP extends ServiceResponse> RESP  badRange(Object entity) {
        // 416 is used for invalid Range requests
        return set(416, entity);
    }

    public final <RESP extends ServiceResponse> RESP  badRequest(Object entity) {
        return set(400, entity);
    }

    public final <RESP extends ServiceResponse> RESP  conflict(Object entity) {
        return set(409, entity);
    }

    public final <RESP extends ServiceResponse> RESP  gone(Object entity) {
        return set(410, entity);
    }
    
    
    public final <RESP extends ServiceResponse> RESP  internalError(Object entity) {
        return set(500, entity);
    }

    public final <RESP extends ServiceResponse> RESP  notFound() {
        return setStatus(404);
    }
    
    public final <RESP extends ServiceResponse> RESP  notFound(Object entity) {
        return set(404, entity);
    }
}
