package com.fasterxml.clustermate.api;

import java.util.List;
import java.util.Map;

/**
 * Immutable class that defines generic API for paths used to make
 * calls using network clients. Network client implementations create
 * {@link RequestPathBuilder}s to use, and accept
 * {@link RequestPath} instances as call targets.
 */
public abstract class RequestPath
{
    /**
     * Factory method for creating builder instance that can
     * be used for building a more refined path, given this
     * instance as the base.
     */
    public abstract <B extends RequestPathBuilder<B>> B builder();

    @Override
    public String toString() {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        RequestPathBuilder b = builder();
        return b.toString();
    }

    /*
    /*********************************************************************
    /* Helper methods for sub-classes
    /*********************************************************************
     */
    
    protected String[] _listToArray(List<String> list)
    {
         if (list == null || list.size() == 0) {
              return null;
         }
         return list.toArray(new String[list.size()]);
    }

    protected Object[] _mapToArray(Map<String,Object> map)
    {
         if (map == null || map.size() == 0) {
              return null;
         }
         Object[] result = new Object[map.size() * 2];
         int ix = 0;
         for (Map.Entry<String,Object> entry : map.entrySet()) {
             result[ix++] = entry.getKey();
             result[ix++] = entry.getValue();
         }
         return result;
    }
}
