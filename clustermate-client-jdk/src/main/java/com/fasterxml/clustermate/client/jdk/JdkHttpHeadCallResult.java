package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.HeadCallResult;

public class JdkHttpHeadCallResult extends HeadCallResult
{
    protected final HttpURLConnection _connection;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public JdkHttpHeadCallResult(HttpURLConnection connection,
            ClusterServerNode server, int status, long contentLength)
    {
        super(server, contentLength);
        _connection = connection;
    }

    public JdkHttpHeadCallResult(HttpURLConnection connection, CallFailure fail) {
        super(fail);
        _connection = connection;
    }

    public JdkHttpHeadCallResult(CallFailure fail) {
        super(fail);
        _connection = null;
    }

    public static JdkHttpHeadCallResult notFound(ClusterServerNode server) {
        return new JdkHttpHeadCallResult(null, server, ClusterMateConstants.HTTP_STATUS_NOT_FOUND, -1);
    }
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

    @Override
    public String getHeaderValue(String key)
    {
        if (_connection != null) {
            return _connection.getHeaderField(key);
        }
        return null;
    }
}
