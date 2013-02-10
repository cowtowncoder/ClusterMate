package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;

import com.fasterxml.clustermate.client.CallFailure;
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
            int status, long contentLength)
    {
        super(status, contentLength);
        _connection = connection;
    }

    public JdkHttpHeadCallResult(CallFailure fail) {
        super(fail);
        _connection = null;
    }

    public static JdkHttpHeadCallResult notFound() {
        return new JdkHttpHeadCallResult(null, 404, -1);
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
