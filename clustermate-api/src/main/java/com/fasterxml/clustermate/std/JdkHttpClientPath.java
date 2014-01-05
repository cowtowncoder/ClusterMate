package com.fasterxml.clustermate.std;

import com.fasterxml.clustermate.api.RequestPath;

/**
 * {@link RequestPath} that {@link JdkHttpClientPathBuilder} creates
 */
public class JdkHttpClientPath<P extends Enum<P>>
    extends RequestPath<P>
{
    protected final String _serverPart;

    protected final String _path;

    protected final String[] _queryParams;

    protected final Object[] _headers;
    
    /**
     * Constructor used by {@link JdkHttpClientPathBuilder}
     */
    public JdkHttpClientPath(JdkHttpClientPathBuilder<P> src) {
        _serverPart = src._serverPart;
        _path = src._path;
        _queryParams = _listToArray(src._queryParams);
        _headers = _mapToArray(src._headers);
    }
    
    @Override
    public JdkHttpClientPathBuilder<P> builder() {
        return new JdkHttpClientPathBuilder<P>(this);
    }
}

