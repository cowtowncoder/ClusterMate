package com.fasterxml.clustermate.api;

import java.util.*;

import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.storemate.shared.StorableKey;

/**
 * Enumeration of types of list items available for listing entries.
 */
public enum ListItemType
{
    /**
     * Entries are ids in their binary form encoded as Base64 in JSON (and raw bytes in Smile).
     * Return type will be <link>StorableKey</link>
     */
    ids(StorableKey.class),
    
    /**
     * Entries are textual representations of ids: may or may not be convertible back
     * to ids, depending on how ids are converted.
     * Return type will be {@link java.lang.String}
     */
    names(String.class),
    
    /**
     * Full object entries that include id (in binary form) as well as additional
     * StoreMate-level metadata.
     */
    entries(ListItem.class);

    private final Class<?> _valueType;
    
    private ListItemType(Class<?> valueType) {
        _valueType = valueType;
    }

    /**
     * Accessor for getting {@link Class} of actual items returned with this
     * logical type.
     */
    public Class<?> getValueType() { return _valueType; }
    
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
