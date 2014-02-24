package com.fasterxml.clustermate.std;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.UTF8UrlEncoder;

/**
 * Simple {@link RequestPathBuilder} implementation that can be used
 * with the default JDK HTTP client.
 */
public class JdkHttpClientPathBuilder
    extends RequestPathBuilder<JdkHttpClientPathBuilder>
{
    protected final static UTF8UrlEncoder _urlEncoder = new UTF8UrlEncoder();
    
    protected final String _serverPart;

    protected String _path;

    protected String _contentType;
    
    protected List<String> _queryParams;

    protected Map<String, Object> _headers;
    
    protected transient URL _url;

    public JdkHttpClientPathBuilder(IpAndPort server) {
        this(server, null, null);
    }

    public JdkHttpClientPathBuilder(String serverPart) {
        this(serverPart, null, (String[]) null);
    }

    public JdkHttpClientPathBuilder(String serverPart, String path) {
        this(serverPart, path, (String[]) null);
    }
    
    public JdkHttpClientPathBuilder(IpAndPort server, String path, String[] qp) {
        this(server.getEndpoint(), path, _arrayToList(qp));
    }

    public JdkHttpClientPathBuilder(String serverPart, String path, String[] qp) {
        this(serverPart, path, _arrayToList(qp));
    }

    public JdkHttpClientPathBuilder(String serverPart, String path, List<String> qp)
    {
        _serverPart = serverPart;
        _path = path;
        _queryParams = qp;
    }

    public JdkHttpClientPathBuilder(JdkHttpClientPath src)
    {
        _serverPart = src._serverPart;
        _path = src._path;
        _queryParams = _arrayToList(src._queryParams);
        _headers = _arrayToMap(src._headers);
    }

    /*
    /*********************************************************************
    /* API impl, mutators
    /*********************************************************************
     */
    
    @Override
    public JdkHttpClientPath build() {
        return new JdkHttpClientPath(this);
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

    @Override
    public String toString() {
         return _url();
    }

    protected String _url()
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
         final int len = _queryParams.size();
         for (int i = 0; i < len; i += 2) {
             sb.append((i == 0) ? '?' : '&');
             sb.append(_queryParams.get(i)).append('=');
             _urlEncoder.appendEncoded(sb, _queryParams.get(i+1), true);
         }
         return sb.toString();
    }
    
    /*
    /*********************************************************************
    /* API impl, mutators
    /*********************************************************************
     */
     
    @Override
    public JdkHttpClientPathBuilder addPathSegment(String segment) {
        return _appendSegment(segment, true);
    }

    @Override
    public JdkHttpClientPathBuilder addPathSegmentsRaw(String segments) {
        return _appendSegment(segments, false);
    }
    
    @Override
    public JdkHttpClientPathBuilder addParameter(String key, String value) {
         _queryParams = _defaultAddParameter(_queryParams, key, value);
         return this;
    }

    @Override
    public JdkHttpClientPathBuilder setContentType(String contentType) {
        _contentType = contentType;
        return this;
    }

    /*
    /*********************************************************************
    /* Extended API
    /*********************************************************************
     */

    @Override
    public JdkHttpClientPathBuilder addHeader(String key, String value) {
        _headers = _defaultAddHeader(_headers, key, value);
        return this;
    }

    @Override
    public JdkHttpClientPathBuilder setHeader(String key, String value) {
        _headers = _defaultSetHeader(_headers, key, value);
        return this;
    }
    
    public URL asURL()
    {
        if (_url == null) {
            String urlStr = _url();
            try {
                _url = new URL(urlStr);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid URL: "+urlStr);
            }
        }
        return _url;
    }

    public void addHeaders(HttpURLConnection conn)
    {
        if (_contentType != null && !_contentType.isEmpty()) {
            conn.setRequestProperty(ClusterMateConstants.HTTP_HEADER_CONTENT_TYPE, _contentType);
        }
        if (_headers != null) {
            for (Map.Entry<String,Object> entry : _headers.entrySet()) {
                String name = entry.getKey();
                Object ob = entry.getValue();
                if (ob instanceof String) {
                    conn.setRequestProperty(name,  (String) ob);
                } else {
                    boolean first = true;
                    for (Object value : (Object[]) ob) {
                        if (first) {
                            conn.setRequestProperty(name, (String) value);
                            first = false;
                        } else {
                            conn.addRequestProperty(name, (String) value);
                        }
                    }
                }
            }
        }
    }
    
    /*
    /*********************************************************************
    /* Internal methods
    /*********************************************************************
     */
    
    protected final JdkHttpClientPathBuilder _appendSegment(String segment, boolean escapeSlash)
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
