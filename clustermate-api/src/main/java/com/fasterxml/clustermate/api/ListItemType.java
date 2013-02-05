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
     * Entries are external textual representations of ids; Strings that can be converted
     * back to raw ids as necessary, but can also be printed out for diagnostics.
     * Return type will be {@link java.lang.String}
     */
    names(String.class),
    
    /**
     * Object entries that include id (in binary form) as well as small set of
     * additional StoreMate-level metadata.
     */
    minimalEntries(ListItem.class),
    
    /**
     * Object entries are {@link ListItem} subtypes that contain additional information
     * that service provides.
     */
    fullEntries(ListItem.class)
    ;

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
