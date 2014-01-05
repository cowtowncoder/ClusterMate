package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.api.RequestPath;

public class AHCPath<P extends Enum<P>>
    extends RequestPath<P>
{
	protected final String _serverPart;

	protected final String _path;

	protected final String[] _queryParams;

	protected final Object[] _headers;

	public AHCPath(AHCPathBuilder<P> src) {
         _serverPart = src._serverPart;
         _path = src._path;
         _queryParams = _listToArray(src._queryParams);
         _headers = _mapToArray(src._headers);
     }

	@Override
	public AHCPathBuilder<P> builder() {
		return new AHCPathBuilder<P>(this);
	}
}
