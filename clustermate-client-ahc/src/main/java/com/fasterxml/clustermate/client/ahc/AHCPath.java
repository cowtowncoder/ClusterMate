package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.api.RequestPath;

public class AHCPath
    extends RequestPath
{
    protected final String _serverPart;

    protected final String _path;

    protected final String[] _queryParams;

    protected final Object[] _headers;

    public AHCPath(AHCPathBuilder src) {
        _serverPart = src._serverPart;
        _path = src._path;
        _queryParams = _listToArray(src._queryParams);
        _headers = _mapToArray(src._headers);
    }

    @SuppressWarnings("unchecked")
    @Override
//    public <B extends RequestPathBuilder<B>> B builder() {
    public AHCPathBuilder builder() {
        return new AHCPathBuilder(this);
    }
}
