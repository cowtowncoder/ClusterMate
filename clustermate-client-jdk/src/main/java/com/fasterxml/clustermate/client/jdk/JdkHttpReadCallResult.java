package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.ReadCallResult;

/**
 * Container for results of a single GET call to a server node.
 * Note that at most one of '_fail' and '_result' can be non-null; however,
 * it is possible for both to be null: this occurs in cases where
 * communication to server(s) succeeds, but no content was found
 * (either 404, or deleted content).
 */
public final class JdkHttpReadCallResult<T> extends ReadCallResult<T>
{
    protected final HttpURLConnection _connection;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */

    public JdkHttpReadCallResult(HttpURLConnection connection, T result) {
        this(connection, ClusterMateConstants.HTTP_STATUS_OK, result);
    }
    
    public JdkHttpReadCallResult(HttpURLConnection connection,
            int status, T result) {
        super(status, result);
        _connection = connection;
    }

    public JdkHttpReadCallResult(CallFailure fail) {
        super(fail);
        _connection = null;
    }

    public static <T> JdkHttpReadCallResult<T> notFound() {
        return new JdkHttpReadCallResult<T>(null, 404, null);
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
