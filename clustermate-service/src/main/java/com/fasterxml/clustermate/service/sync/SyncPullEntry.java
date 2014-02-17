package com.fasterxml.clustermate.service.sync;

import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

/**
 * Helper class we use as an intermediary for per-entry metadata chunk
 * and raw metadata entry.
 */
public class SyncPullEntry
{
    // External key for accessing the entry
    public StorableKey key;

    public long creationTime;

    public int minTTLSecs, maxTTLSecs;

    public long size, storageSize;

    public int checksum, checksumForCompressed;
    
    public Compression compression;

    public boolean isDeleted;

    public byte lastAccessMethod;

    public SyncPullEntry() { }

    SyncPullEntry(StoredEntry<?> src)
    {
        final Storable raw = src.getRaw();
        key = src.getKey().asStorableKey();
        creationTime = src.getCreationTime();
        checksum = raw.getContentHash();
        checksumForCompressed = raw.getCompressedHash();
        
        if (src.isDeleted()) {
            storageSize = -1L;
            size = -1L;
        } else {
            storageSize = raw.getStorageLength();
            size = raw.getOriginalLength();
        }
        compression = raw.getCompression();
        LastAccessUpdateMethod m = src.getLastAccessUpdateMethod();
        lastAccessMethod = (m == null) ? 0 : m.asByte();
        minTTLSecs = src.getMinTTLSinceAccessSecs();
        maxTTLSecs = src.getMaxTTLSecs();
        
        isDeleted = src.isDeleted();
    }
}