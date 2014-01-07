package com.fasterxml.clustermate.api;

import java.util.*;

import com.fasterxml.storemate.shared.compress.Compression;

/**
 * Builder object used for constructing {@link RequestPath}
 * instances.
 */
public abstract class RequestPathBuilder<
    THIS extends RequestPathBuilder<THIS>
>
{
    /**
     * Method that will construct the immutable {@link RequestPath} instance
     * with information builder has accumulated.
     */
    public abstract RequestPath build();

    /*
    /*********************************************************************
    /* Mutators
    /*********************************************************************
     */
    
    /**
     * Method that will append a single path segment, escaping characters
     * as necessary, including escaping of slash character so that
     * the whole value remains parseable path segment.
     *
     * @return Builder instance to use for further calls (may be 'this',
     *   but does not have to be)
     */
    public abstract THIS addPathSegment(String segment);

    /**
     * Method that will append one or more path segments; contents are
     * only escaped only for mandatory characters, but slashes are NOT
     * escaped, so input may contain multiple segments
     *
     * @return Builder instance to use for further calls (may be 'this',
     *   but does not have to be)
     */
    public abstract THIS addPathSegmentsRaw(String segments);

    /**
     * Convenience method for appending a sequence of path segments,
     * as if calling {@link #addPathSegment(String)} once per segment,
     * in the order indicated.
     */
    public THIS addPathSegments(String[] segments)
    {
        RequestPathBuilder<THIS> builder = this;
        for (String segment : segments) {
            builder = builder.addPathSegment(segment);
        }
        return _this(builder);
    }
    
    /**
     * Method that will add a single query parameter in the logical
     * path, with given value.
     *
     * @return Builder instance to use for further calls (may be 'this',
     *   but does not have to be)
     */
    public abstract THIS addParameter(String key, String value);

    /**
     * Convenience method, functionally equivalent to:
     *<pre>
     *  addParameter(String.valueOf(value));
     *</pre>
     */
    public THIS addParameter(String key, int value) {
        return addParameter(key, String.valueOf(value));
    }

    /**
     * Convenience method, functionally equivalent to:
     *<pre>
     *  addParameter(String.valueOf(value));
     *</pre>
     */
    public THIS addParameter(String key, long value) {
        return addParameter(key, String.valueOf(value));
    }

    public abstract THIS addHeader(String key, String value);
    
    public THIS addHeader(String key, long value) {
        return addHeader(key, String.valueOf(value));
    }

    public abstract THIS setHeader(String key, String value);

    public THIS setHeader(String key, long value) {
        return setHeader(key, String.valueOf(value));
    }
    
    public THIS addCompression(Compression comp, long originalLength) {
        if (comp != null) {
            THIS req = addHeader(ClusterMateConstants.HTTP_HEADER_COMPRESSION,
                    comp.asContentEncoding());
            if (comp != Compression.NONE) {
                req = req.addHeader(ClusterMateConstants.CUSTOM_HTTP_HEADER_UNCOMPRESSED_LENGTH,
                        originalLength);
            }
            return req;
        }
        return _this();
    }
    
    /*
    /*********************************************************************
    /* Accessors
    /*********************************************************************
     */

    /**
     * Method for returning only the logical "server part", which also includes
     * the protocol (like 'http') and port number, as well as trailing
     * slash.
     */
    public abstract String getServerPart();

    /**
     * Method for returning only the logical "path" part, without including
     * either server part or query parameters.
     */
    public abstract String getPath();

    public abstract boolean hasHeaders();
    
    /**
     * Implementations MUST override this to produce a valid URL that
     * represents the current state of builder.
     */
    @Override
    public abstract String toString();

    /*
    /*********************************************************************
    /* Helper methods for sub-classes
    /*********************************************************************
     */

    @SuppressWarnings("unchecked")
    protected THIS _this() {
        return (THIS) this;
    }
    @SuppressWarnings("unchecked")
    protected THIS _this(RequestPathBuilder<THIS> b) {
        return (THIS) b;
    }

    protected static List<String> _arrayToList(String[] qp)
    {
         if (qp == null) {
             return null;
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

    protected static Map<String,Object> _arrayToMap(Object[] qp)
    {
         if (qp == null) {
             return null;
         }
         int len = qp.length;
         Map<String,Object> result = new LinkedHashMap<String,Object>(len >> 1);
         for (int i = 0; i < len; i += 2) {
             result.put((String) qp[i], qp[i+1]); 
         }
         return result;
    }

    protected Map<String,Object> _newHeaderMap() {
        return new LinkedHashMap<String,Object>(8);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> _defaultAddHeader(Map<String, Object> headers,
        String key, String value)
    {
        if (headers == null) {
            headers = _newHeaderMap();
            headers.put(key, value);
            return headers;
        }
        Object ob = headers.get(key);
        if (ob == null) {
            headers.put(key, value);
        } else {
            List<Object> l;
            if (ob instanceof String) {
                l = new ArrayList<Object>(4);
                l.add(ob);
            } else {
                l = (List<Object>) ob;
            }
            l.add(value);
        }
        return headers;
    }

    protected Map<String, Object> _defaultSetHeader(Map<String, Object> headers,
        String key, String value)
    {
        if (headers == null) {
            headers = _newHeaderMap();
        }
        headers.put(key, value);
        return headers;
    }

    protected List<String> _defaultAddParameter(List<String> qp,
            String key, String value)
    {
        if (qp == null) {
            qp = new ArrayList<String>(8);
        }
        qp.add(key);
        qp.add(value);
        return qp;
    }
}
