package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.ReadCallResult;

/**
 * Container for results of a single GET call to a server node.
 * Note that at most one of '_fail' and '_result' can be non-null; however,
 * it is possible for both to be null: this occurs in cases where
 * communication to server(s) succeeds, but no content was found
 * (either 404, or deleted content).
 */
public class JdkHttpReadCallResult<T> extends ReadCallResult<T>
{
    protected final HttpURLConnection _connection;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */

    public JdkHttpReadCallResult(HttpURLConnection connection,
            ClusterServerNode server, T result) {
        super(server, result);
        _connection = connection;
    }
    
    public JdkHttpReadCallResult(HttpURLConnection connection,
            ClusterServerNode server, int status, T result) {
        super(server, status, result);
        _connection = connection;
    }

    public JdkHttpReadCallResult(HttpURLConnection connection, CallFailure fail) {
        super(fail);
        _connection = connection;
    }

    public static <T> JdkHttpReadCallResult<T> notFound(ClusterServerNode server) {
        return new JdkHttpReadCallResult<T>(null, server,
                ClusterMateConstants.HTTP_STATUS_NOT_FOUND, null);
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
