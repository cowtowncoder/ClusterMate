package com.fasterxml.clustermate.service;

import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.msg.StreamingResponseContent;
import com.fasterxml.storemate.shared.HTTPConstants;


/**
 * Interface class that defines interface of (HTTP) Responses
 * server returns to caller.
 *<p>
 * Separated out to allow handlers to operate independent of the
 * container like Servlet or JAX-RS container.
 */
public abstract class ServiceResponse
{
    protected Object _entity;

    protected StreamingResponseContent _streamingContent;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Low-level response building
    ///////////////////////////////////////////////////////////////////////
     */
    
    public abstract ServiceResponse set(int code, Object entity);

    public abstract ServiceResponse setStatus(int code);
    
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
    public final <T extends ServiceResponse> T setEntity(Object e)
    {
        if (e instanceof StreamingResponseContent) {
            _entity = null;
            _streamingContent = (StreamingResponseContent) e;
        } else {
            _entity = e;
            _streamingContent = null;
        }
        return (T) this;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Basic accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public abstract int getStatus();

    public final boolean isError() { return getStatus() >= 300; }

    public final boolean hasEntity() { return _entity != null; }
    public final boolean hasStreamingContent() { return _streamingContent != null; }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // High(er)-level response building; semantic headers
    ///////////////////////////////////////////////////////////////////////
     */

    public abstract ServiceResponse setContentType(String contentType);

    public ServiceResponse setContentTypeJson() {
        return setContentType("application/json");
    }
    
    public final ServiceResponse setBodyCompression(String type) {
        return addHeader(HTTPConstants.HTTP_HEADER_COMPRESSION, type);
    }
    
    public abstract ServiceResponse setContentLength(long length);

    /*
    ///////////////////////////////////////////////////////////////////////
    // High(er)-level response building; ok cases
    ///////////////////////////////////////////////////////////////////////
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
        return set(HTTPConstants.HTTP_STATUS_OK_PARTIAL, entity)
                .addHeader(HTTPConstants.HTTP_HEADER_RANGE_FOR_RESPONSE, rangeDesc);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // High(er)-level response building; error cases
    ///////////////////////////////////////////////////////////////////////
     */
    
    public final ServiceResponse badRange(Object entity) {
        // 416 is used for invalid Range requests
        return set(416, entity);
    }

    public final ServiceResponse badRequest(Object entity) {
        return set(400, entity);
    }

    public final ServiceResponse conflict(Object entity) {
        return set(409, entity);
    }

    public final ServiceResponse gone(Object entity) {
        return set(410, entity);
    }
    
    
    public final ServiceResponse internalError(Object entity) {
        return set(500, entity);
    }

    public final ServiceResponse notFound(Object entity) {
        return set(404, entity);
    }
}
