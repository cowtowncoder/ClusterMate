package com.fasterxml.clustermate.api;

import java.util.*;

/**
 * Enumeration of types of list items available for listing entries.
 */
public enum ListItemType
{
    /**
     * Entries are ids in their binary form encoded as Base64 in JSON (and raw bytes in Smile)
     */
    ids,
    
    /**
     * Entries are textual representations of ids: may or may not be convertible back
     * to ids, depending on how ids are converted.
     */
    names,
    
    /**
     * Full object entries that include id (in binary form) as well as additional
     * StoreMate-level metadata.
     */
    entries;

    private final static HashMap<String,ListItemType> _entries = new HashMap<String,ListItemType>();
    static {
        for (ListItemType t : ListItemType.values()) {
            _entries.put(t.name(), t);
        }
    }

    public static ListItemType find(String str)
    {
        return _entries.get(str);
    }
}
