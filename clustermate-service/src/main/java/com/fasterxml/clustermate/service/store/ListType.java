package com.fasterxml.clustermate.service.store;

import java.util.*;

/**
 * Enumeration of types of list items when listing entries.
 */
public enum ListType
{
    /**
     * Binary entries; encoded as Base64 in JSON (and raw bytes in Smile)
     */
    ids,
    
    /**
     * Textual representation of ids: may or may not be convertible back
     * to ids.
     */
    names,
    
    /**
     * Full entries
     */
    entries;

    private final static HashMap<String,ListType> _entries = new HashMap<String,ListType>();
    static {
        for (ListType t : ListType.values()) {
            _entries.put(t.name(), t);
        }
    }

    public static ListType find(String str)
    {
        return _entries.get(str);
    }
}
