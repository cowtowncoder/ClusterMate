package com.fasterxml.clustermate.service.store;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.store.Storable;

/**
 * Converter used by store for handling conversions between
 * low-level {@link Storable} (things stored in StoreMate store),
 * and ClusterMate-level {@link StoredEntry} instances.
 *<p>
 * Custom sub-classing is needed to support additional metadata for
 * systems implementations.
 */
public abstract class StoredEntryConverter<K extends EntryKey,
    E extends StoredEntry<K>,
    L extends ListItem
>
{
    // // // Key conversion

    public abstract EntryKeyConverter<K> keyConverter();
    
    // // // Entry conversion
    
    public abstract E entryFromStorable(Storable raw);

    public abstract E entryFromStorable(K key, final Storable raw);

    public abstract E entryFromStorable(K key, Storable raw,
            byte[] buffer, int offset, int length);

    /**
     * Method for constructing implementation specific {@link ListItem} instances
     * (subtypes) from raw {@link Storable}
     */
    public abstract L listItemFromStorable(Storable raw);
    
    // // // Metadata handling
    
    /**
     * Method called to construct "custom metadata" section to be
     * used for constructing a new <code>Storable</code> instance.
     */
    public abstract ByteContainer createMetadata(long creationTime,
            byte lastAccessUpdateMethod,
            int minTTLSecs, int maxTTLSecs);
    
    // // // Last accessed
    
    public abstract EntryLastAccessed createLastAccessed(E entry, long accessTime);

    public abstract EntryLastAccessed createLastAccessed(byte[] raw);

    /*
    /**********************************************************************
    /* Default implementations (mostly for unit tests)
    /**********************************************************************
     */

    /**
     * Helper method that can be used as the baseline implementation for 
     * {@link #listItemFromStorable(Storable)} by tests.
     */
    protected ListItem defaultListItemFromStorable(Storable raw) {
        return new ListItem(raw.getKey(), raw.getContentHash(), raw.getActualUncompressedLength());
    }
}
