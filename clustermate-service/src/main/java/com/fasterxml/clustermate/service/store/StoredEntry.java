package com.fasterxml.clustermate.service.store;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

/**
 * POJO for storing metadata for a single file entry.
 */
public abstract class StoredEntry<K extends EntryKey>
{
    /*
    /**********************************************************************
    /* Public API, field access
    /**********************************************************************
     */

    public abstract K getKey();
    
    public abstract long getCreationTime();
    public abstract LastAccessUpdateMethod getLastAccessUpdateMethod();
    public abstract int getMinTTLSinceAccessSecs();
    public abstract int getMaxTTLSecs();
    
    /*
    /**********************************************************************
    /* Public API, derived methods
    /**********************************************************************
     */

    /**
     * Method that can be called to verify whether the entry has exceeded its
     * maximum time-to-live, and can then be expired. Note that getting a
     * 'false' does NOT necessarily mean that entry has not expired, since
     * figuring that out requires knowledge of "last-accessed" set up, if any.
     * 'True' will, however, mean that entry may be removed locally (and should
     * eventually be removed by other replicas as well).
     * 
     * @param currentTime Current system time provided by caller
     * 
     * @return True if the entry is known to have exceeded its maximum time-to-live;
     *   false if this is not known.
     */
    public abstract boolean hasExceededMaxTTL(long currentTime);

    /**
     * Method that can be used to check whether entry has exceeded the absolute
     * minimum time-to-live (regardless of last-access) or not.
     * This can be used as an optimization to avoid having to access
     * (local) last-access time.
     */
    public abstract boolean hasExceededMinTTL(long currentTime);
    
    /**
     * Method that can be called to check whether this entry uses last-access
     * timestamp for determining expiration.
     */
    public abstract boolean usesLastAccessTime();
    
    /**
     * Method that can be called to check whether entry is expired
     * with respect to local last-access.
     *<p>
     * Note: getting 'true' 
     * 
     * @param currentTime Current system time provided by caller
     * @param lastAccess Timestamp of the last <b>local</b> access
     * 
     * @return Whether entry has exceeded its TTL based only on the local
     *    last-access settings and information
     */
    public abstract boolean hasExceededLastAccessTTL(long currentTime, long lastAccess);
    public abstract boolean insertedBefore(long timestamp);

    public abstract boolean createdBefore(long timestamp);

    public abstract int routingHashUsing(EntryKeyConverter<K> hasher);

    /**
     * Method that calculates conservative estimation on when this entry will
     * be expired for sure. Typically used for expiration time for last-accessed
     * entries (if used)
     * 
     * @since 0.9.6
     */
    public long calculateMaxExpirationTime() {
        return getCreationTime() + (1000 * getMaxTTLSecs());
    }
    
    /*
    /**********************************************************************
    /* Public API, pass-through methods
    /**********************************************************************
     */
    
    public abstract Storable getRaw();

    public boolean isDeleted() {
        return getRaw().isDeleted();
    }

    public boolean hasExternalData() {
        return getRaw().hasExternalData();
    }

    public boolean hasInlineData() {
        return getRaw().hasInlineData();
    }
    
    public Compression getCompression() {
        return getRaw().getCompression();
    }

    public long getLastModifiedTime() {
        return getRaw().getLastModified();
    }
    
    public long getStorageLength() {
        return getRaw().getStorageLength();
    }

    public long getActualUncompressedLength() {
        return getRaw().getActualUncompressedLength();
    }
}
