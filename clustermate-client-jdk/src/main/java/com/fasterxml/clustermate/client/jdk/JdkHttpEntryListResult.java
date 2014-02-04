package com.fasterxml.clustermate.client.jdk;

import java.net.HttpURLConnection;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.ListCallResult;

public class JdkHttpEntryListResult<T> extends ListCallResult<T>
{
    protected final HttpURLConnection _connection;
    
    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public JdkHttpEntryListResult(HttpURLConnection conn, ListResponse<T> resp) {
        super(resp);
        _connection = conn;
    }

    public JdkHttpEntryListResult(CallFailure fail) {
        super(fail);
        _connection = null;
    }

    public JdkHttpEntryListResult(int failCode) {
        super(failCode);
        _connection = null;
    }
    
    public static <T> JdkHttpEntryListResult<T> notFound() {
        return new JdkHttpEntryListResult<T>(ClusterMateConstants.HTTP_STATUS_NOT_FOUND);
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
