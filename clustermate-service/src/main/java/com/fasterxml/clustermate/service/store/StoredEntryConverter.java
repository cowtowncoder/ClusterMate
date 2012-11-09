package com.fasterxml.clustermate.service.store;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.store.Storable;

/**
 * Converter used by Vagabond store for handling conversions between
 * low-level {@link Storable} (things stored in StoreMate store),
 * and Vagabond-level {@link StoredEntry} instances.
 *<p>
 * Custom sub-classing is needed to support additional metadata for
 * Vagabond-based systems.
 */
public abstract class StoredEntryConverter<K extends EntryKey, E extends StoredEntry<K>>
{
    // // // Key conversion

    public abstract EntryKeyConverter<K> keyConverter();
    
    // // // Entry conversion
    
    public abstract E entryFromStorable(final Storable raw);

    public abstract E entryFromStorable(final K key, final Storable raw);

    public abstract E entryFromStorable(K key, Storable raw,
            byte[] buffer, int offset, int length);

    // // // Metadata handling
    
    /**
     * Method called to construct "custom metadata" section to be
     * used for constructing a new <code>Storable</code> instance.
     */
    public abstract ByteContainer createMetadata(long creationTime,
            LastAccessUpdateMethod lastAccessUpdateMethod,
            int minTTLSecs, int maxTTLSecs);
    
    // // // Last accessed
    
    public abstract EntryLastAccessed createLastAccessed(E entry, long accessTime);

    public abstract EntryLastAccessed createLastAccessed(byte[] raw);

}
