package com.fasterxml.clustermate.std;

import com.fasterxml.clustermate.api.RequestPath;

/**
 * {@link RequestPath} that {@link JdkHttpClientPathBuilder} creates
 */
public class JdkHttpClientPath extends RequestPath
{
    protected final String _serverPart;

    protected final String _path;

    protected final String[] _queryParams;

    protected final Object[] _headers;
    
    /**
     * Constructor used by {@link JdkHttpClientPathBuilder}
     */
    public JdkHttpClientPath(JdkHttpClientPathBuilder src) {
        _serverPart = src._serverPart;
        _path = src._path;
        _queryParams = _listToArray(src._queryParams);
        _headers = _mapToArray(src._headers);
    }
    
    @Override
    public JdkHttpClientPathBuilder builder() {
        return new JdkHttpClientPathBuilder(this);
    }
}

