package com.fasterxml.clustermate.client.ahc;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.RequestPathBuilder;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.ning.http.util.UTF8UrlEncoder;

public class AHCPathBuilder
	extends RequestPathBuilder
{
	// Server definition won't change, can make final
	protected final String _serverPart;

	protected String _path;

	protected List<String> _queryParams;
	
	public AHCPathBuilder(IpAndPort server) {
		this(server, null, null);
	}

	public AHCPathBuilder(IpAndPort server, String path, String[] qp) {
		this(server.getEndpoint(), path, _arrayToList(qp));
	}

	public AHCPathBuilder(String serverPart, String path, String[] qp) {
		this(serverPart, path, _arrayToList(qp));
	}
	
	public AHCPathBuilder(String serverPart, String path, List<String> qp)
	{
		_serverPart = serverPart;
		_path = path;
		_queryParams = qp;
	}

	private static List<String> _arrayToList(String[] qp)
	{
		if (qp == null) {
			return new ArrayList<String>(8);
		}
		int len = qp.length;
		List<String> list = new ArrayList<String>(Math.min(8, len));
		if (len > 0) {
			for (int i = 0; i < len; ++i) {
				list.add(qp[i]);
			}
		}
		return list;
	}

    /*
    ///////////////////////////////////////////////////////////////////////
    // API impl
    ///////////////////////////////////////////////////////////////////////
     */
	
	@Override
	public RequestPathBuilder addPathSegment(String segment)
	{
		if (_path == null) {
			_path = UTF8UrlEncoder.encode(segment);
		} else {
			StringBuilder sb = new StringBuilder(_path);
			sb.append('/');
			if (segment != null && segment.length() > 0) {
				sb = UTF8UrlEncoder.appendEncoded(sb, segment);
			}
			_path = sb.toString();
		}
		return this;
	}

	@Override
	public RequestPathBuilder addParameter(String key, String value)
	{
		if (_queryParams == null) {
			_queryParams = new ArrayList<String>(8);
		}
		_queryParams.add(key);
		_queryParams.add(value);
		return this;
	}

	@Override
	public AHCPath build() {
		return new AHCPath(_serverPart, _path, _queryParams);
	}

    /*
    ///////////////////////////////////////////////////////////////////////
    // Extended API
    ///////////////////////////////////////////////////////////////////////
     */
	
	public BoundRequestBuilder putRequest(AsyncHttpClient ahc) {
		return _addParams(ahc.preparePut(toString()));
	}

	public BoundRequestBuilder getRequest(AsyncHttpClient ahc) {
		return _addParams(ahc.prepareGet(toString()));
	}

	public BoundRequestBuilder headRequest(AsyncHttpClient ahc) {
		return _addParams(ahc.prepareHead(toString()));
	}

     public BoundRequestBuilder deleteRequest(AsyncHttpClient ahc) {
         return _addParams(ahc.prepareDelete(toString()));
    }
	
	private BoundRequestBuilder _addParams(BoundRequestBuilder b)
	{
		if (_queryParams != null) {
			for (int i = 0, len = _queryParams.size(); i < len; i += 2) {
				b = b.addQueryParameter(_queryParams.get(i), _queryParams.get(i+1));
			}
		}
		return b;
	}

	protected String _url() {
		if (_path == null) {
			return _serverPart;
		}
		return _serverPart + _path;
	}

	@Override
	public String toString() {
		return _url();
	}
}
