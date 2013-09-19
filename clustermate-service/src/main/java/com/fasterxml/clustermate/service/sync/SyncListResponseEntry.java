package com.fasterxml.clustermate.service.sync;

import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;

/**
 * Helper POJO to contain details of individual syncable entry, in minimal
 * form: just enough to know if a sync is needed.
 */
public class SyncListResponseEntry
{
    // External key for accessing the entry
    public StorableKey key;

    // (storage) size needed for estimating how many we should request
    public long size;

    // Of timestamps, just need 'insertionTime' (i.e. last state modification);
    // creationTime only needed if we do sync
    public long insertionTime;

    // Content hash of the entry
    public int hash;
    
    static SyncListResponseEntry valueOf(StoredEntry<?> src)
    {
        SyncListResponseEntry e = new SyncListResponseEntry();
        e.key = src.getKey().asStorableKey();
        Storable raw = src.getRaw();
        e.insertionTime = raw.getLastModified();
        e.size = raw.isDeleted() ? -1L : raw.getStorageLength();
        e.hash = raw.getContentHash();
        return e;
    }

    // use 'non-getter' name to avoid getting serialized
    public boolean deleted() {
        return size < 0L;
    }
}