package com.fasterxml.clustermate.service.store;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.msg.ItemInfo;
import com.fasterxml.clustermate.api.msg.ListItem;

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
    /* 15-Feb-2014, tatu: Only now realized that this type parameter is
     *   pretty much useless; could/should eliminate. But major refactoring
     *   to get rid of it...
     */
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
     * Method for constructing implementation minimal {@link ListItem} instances
     * (subtypes) from raw {@link Storable}
     */
    public ListItem minimalListItemFromStorable(Storable raw) {
        return defaultMinimalListItemFromStorable(raw);
    }

    /**
     * Method for constructing implementation implementation-specific
     * full {@link ListItem} instances (subtypes) from raw {@link Storable}
     */
    public abstract L fullListItemFromStorable(Storable raw);

    /**
     * Method for constructing {@link ItemInfo} from raw entry metadata
     */
    public ItemInfo itemInfoFromStorable(Storable raw) {
        return defaultItemInfoFromStorable(raw);
    }
    
    // // // Metadata handling
    
    /**
     * Method called to construct "custom metadata" section to be
     * used for constructing a new <code>Storable</code> instance.
     */
    public abstract ByteContainer createMetadata(long creationTime,
            byte lastAccessUpdateMethod,
            int minTTLSecs, int maxTTLSecs);
    
    /*
    /**********************************************************************
    /* Default implementations (mostly for unit tests)
    /**********************************************************************
     */

    /**
     * Helper method that can be used as the baseline implementation for 
     * {@link #listItemFromStorable(Storable)} by tests.
     */
    protected ListItem defaultMinimalListItemFromStorable(Storable raw) {
        return new ListItem(raw.getKey(), raw.getContentHash(), raw.getActualUncompressedLength());
    }

    protected ItemInfo defaultItemInfoFromStorable(Storable raw) {
        Compression c = raw.getCompression();
        Character compression = (c == null || c == Compression.NONE) ? null
                : Character.valueOf(c.asChar());
        long compLen = (compression == null) ? -1L : raw.getStorageLength();
        StringBuilder sb = new StringBuilder(4);
        if (raw.isDeleted()) {
            sb.append(ItemInfo.FLAG_DELETED);
        }
        if (!raw.hasExternalData()) {
            sb.append(ItemInfo.FLAG_INLINED);
        }
        if (raw.isReplicated()) {
            sb.append(ItemInfo.FLAG_REPLICA);
        }
        String flags = (sb.length() == 0) ? "" : sb.toString();
        return new ItemInfo(raw.getLastModified(), raw.getActualUncompressedLength(), compLen,
                compression, raw.getContentHash(),
                flags);
    }
}
