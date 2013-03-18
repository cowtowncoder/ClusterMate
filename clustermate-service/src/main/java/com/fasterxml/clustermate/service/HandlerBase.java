package com.fasterxml.clustermate.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ContentType;
import com.fasterxml.storemate.shared.IpAndPort;

public abstract class HandlerBase
{
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    /*
    /**********************************************************************
    /* Helper methods, query param handling
    /**********************************************************************
     */

    protected Integer _findIntParam(ServiceRequest request, String key)
    {
        String str = request.getQueryParameter(key);
        if (str == null || (str = str.trim()).isEmpty()) {
            return null;
        }
        try {
            return Integer.valueOf(str);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    protected long _findLongParam(ServiceRequest request, String key)
    {
        String str = request.getQueryParameter(key);
        if (str == null || (str = str.trim()).isEmpty()) {
            return 0L;
        }
        try {
            return Long.valueOf(str);
        } catch (IllegalArgumentException e) {
            return 0L;
        }
    }
    
    protected IpAndPort getCallerQueryParam(ServiceRequest request)
    {
        String str = request.getQueryParameter(ClusterMateConstants.QUERY_PARAM_CALLER);
        if (str == null || (str = str.trim()).length() == 0) {
            return null;
        }
        try {
            return new IpAndPort(str);
        } catch (Exception e) {
            LOG.warn("Invalid value for {}: '{}', problem: {}",
                    ClusterMateConstants.QUERY_PARAM_CALLER,
                    str, e.getMessage());
            return null;
        }
    }

    /*
    /**********************************************************************
    /* Helper methods, header handling
    /**********************************************************************
     */
    
    protected boolean _acceptSmileContentType(ServiceRequest request) {
        String acceptHeader = request.getHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT);
        // what do they request? If not known, serve JSON (assumed to be from browser)
        return (acceptHeader != null) && ContentType.SMILE.isAccepted(acceptHeader);
    }
    
    /*
    /**********************************************************************
    /* Helper methods, error processing
    /**********************************************************************
     */
    
    // public since it's called from SyncListServlet
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT missingArgument(ServiceResponse response, String argId) {
        return (OUT) badRequest(response, "Missing query parameter '"+argId+"'");
    }

    // public since it's called from SyncListServlet
    @SuppressWarnings("unchecked")
    public <OUT extends ServiceResponse> OUT invalidArgument(ServiceResponse response, String argId, String argValue)
    {
        if (argValue == null) {
            return (OUT) missingArgument(response, argId);
        }
        return (OUT) badRequest(response, "Invalid query parameter '"+argId+"': value '"+argValue+"'");
    }

    protected <OUT extends ServiceResponse> OUT badRequest(ServiceResponse response, String errorTemplate, Object... args) {
        String msg = (args == null || args.length == 0) ? errorTemplate : String.format(errorTemplate, args);
        return _badRequest(response, msg);
    }
    
    protected abstract <OUT extends ServiceResponse> OUT _badRequest(ServiceResponse response, String msg);
}
