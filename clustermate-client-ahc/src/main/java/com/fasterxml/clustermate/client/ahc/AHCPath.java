package com.fasterxml.clustermate.client.ahc;

import java.util.List;

import com.fasterxml.storemate.shared.RequestPath;

public class AHCPath extends RequestPath
{
	protected final String _serverPart;

	protected final String _path;

	protected String[] _queryParams;
	
	public AHCPath(String serverPart, String path, List<String> qp) {
		_serverPart = serverPart;
		_path = path;
		_queryParams = _listToArray(qp);
	}

	@Override
	public AHCPathBuilder builder() {
		return new AHCPathBuilder(_serverPart, _path, _queryParams);
	}

	private String[] _listToArray(List<String> list)
	{
		if (list == null || list.size() == 0) {
			return null;
		}
		return list.toArray(new String[list.size()]);
	}
}
