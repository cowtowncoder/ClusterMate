package com.fasterxml.clustermate.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.storemate.shared.IpAndPort;

public class HandlerBase
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
        String str = request.getQueryParameter(ClusterMateConstants.HTTP_QUERY_PARAM_CALLER);
        if (str == null || (str = str.trim()).length() == 0) {
            return null;
        }
        try {
            return new IpAndPort(str);
        } catch (Exception e) {
            LOG.warn("Invalid value for {}: '{}', problem: {}",
                    ClusterMateConstants.HTTP_QUERY_PARAM_CALLER,
                    str, e.getMessage());
            return null;
        }
    }
}
