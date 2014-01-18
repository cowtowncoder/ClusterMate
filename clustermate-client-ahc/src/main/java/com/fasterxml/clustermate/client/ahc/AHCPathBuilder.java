package com.fasterxml.clustermate.client.ahc;

import java.util.*;

import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.UTF8UrlEncoder;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

public class AHCPathBuilder
	extends RequestPathBuilder<AHCPathBuilder>
{
    protected final static UTF8UrlEncoder _urlEncoder = new UTF8UrlEncoder();

    protected final String _serverPart;

    protected String _path;

    protected List<String> _queryParams;

    protected Map<String, Object> _headers;
    
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

    public AHCPathBuilder(AHCPath src)
    {
        _serverPart = src._serverPart;
        _path = src._path;
        _queryParams = _arrayToList(src._queryParams);
        _headers = _arrayToMap(src._headers);
    }
    
//  public <P extends Enum<P>, B extends RequestPathBuilder<P,B>> B builder();
    @Override
    public AHCPath build() {
        return new AHCPath(this);
    }
    
    /*
    /*********************************************************************
    /* API impl, accessors
    /*********************************************************************
     */
    
    @Override
    public String getServerPart() {
        return _serverPart;
    }

    @Override
    public String getPath() {
        return _path;
    }

    @Override
    public boolean hasHeaders() {
        return (_headers != null) && !_headers.isEmpty();
    }

    /*
    /*********************************************************************
    /* API impl, 
    /*********************************************************************
     */
	
    @Override
    public AHCPathBuilder addPathSegment(String segment) {
        return _appendSegment(segment, true);
    }

    @Override
    public AHCPathBuilder addPathSegmentsRaw(String segments) {
        return _appendSegment(segments, false);
    }
	
    @Override
    public AHCPathBuilder addParameter(String key, String value) {
        _queryParams = _defaultAddParameter(_queryParams, key, value);
        return this;
    }

    /*
    /*********************************************************************
    /* Extended API
    /*********************************************************************
     */

    @Override
    public AHCPathBuilder addHeader(String key, String value) {
        _headers = _defaultAddHeader(_headers, key, value);
        return this;
    }

    @Override
    public AHCPathBuilder setHeader(String key, String value) {
        _headers = _defaultSetHeader(_headers, key, value);
        return this;
    }

    public BoundRequestBuilder putRequest(AsyncHttpClient ahc) {
        return _addParamsAndHeaders(ahc.preparePut(_url(false)));
    }

    public BoundRequestBuilder getRequest(AsyncHttpClient ahc) {
        return _addParamsAndHeaders(ahc.prepareGet(_url(false)));
    }

    public BoundRequestBuilder headRequest(AsyncHttpClient ahc) {
        return _addParamsAndHeaders(ahc.prepareHead(_url(false)));
    }

    public BoundRequestBuilder deleteRequest(AsyncHttpClient ahc) {
        return _addParamsAndHeaders(ahc.prepareDelete(_url(false)));
    }

    public BoundRequestBuilder listRequest(AsyncHttpClient ahc) {
        return _addParamsAndHeaders(ahc.prepareGet(_url(false)));
    }

    private BoundRequestBuilder _addParamsAndHeaders(BoundRequestBuilder reqBuilder)
    {
        if (_headers != null) {
            for (Map.Entry<String,Object> entry : _headers.entrySet()) {
                String name = entry.getKey();
                Object ob = entry.getValue();
                if (ob instanceof String) {
                    reqBuilder = reqBuilder.setHeader(name,  (String) ob);
                } else {
                    boolean first = true;
                    for (Object value : (Object[]) ob) {
                        if (first) {
                            reqBuilder = reqBuilder.setHeader(name,  (String) value);
                            first = false;
                        } else {
                            reqBuilder = reqBuilder.addHeader(name,  (String) value);
                        }
                    }
                }
            }
        }
        if (_queryParams != null) {
            for (int i = 0, len = _queryParams.size(); i < len; i += 2) {
                reqBuilder = reqBuilder.addQueryParameter(_queryParams.get(i), _queryParams.get(i+1));
            }
        }
        return reqBuilder;
    }
    
    protected String _url(boolean addQueryParams)
    {
		if (_path == null) {
			return _serverPart;
		}
		if (_queryParams == null) {
		    return _serverPart + _path;
		}
		StringBuilder sb = new StringBuilder(100);
		sb.append(_serverPart);
		sb.append(_path);
		if (addQueryParams) {
		    final int len = _queryParams.size();
		    if (len > 0) {
    		        sb.append('?');
    		        for (int i = 0; i < len; i += 2) {
    		            sb.append(_queryParams.get(i)).append('=');
                      _urlEncoder.appendEncoded(sb, _queryParams.get(i+1), true);
                  }
		    }
    		}
          return sb.toString();
    }

    @Override
    public String toString() {
        return _url(true);
    }

    protected final AHCPathBuilder _appendSegment(String segment, boolean escapeSlash)
    {
        if (_path == null) {
            _path = _urlEncoder.encode(segment, escapeSlash);
        } else {
            StringBuilder sb = new StringBuilder(_path);
            sb.append('/');
            if (segment != null && segment.length() > 0) {
                sb = _urlEncoder.appendEncoded(sb, segment, escapeSlash);
            }
            _path = sb.toString();
        }
        return this;
    }
}
