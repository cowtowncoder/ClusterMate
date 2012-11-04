package com.fasterxml.clustermate.service.store;

import com.fasterxml.clustermate.service.bdb.BDBConverters;

/**
 * POJO for storing simple last-accessed timestamp, associated with
 * one or more entries.
 *<p>
 * Internal structure is:
 *<ol>
 * <li>#0: long lastAccessTime
 * <li>#8: long entryCreationTime
 * <li>#16: long type
 *</ol>
 * giving fixed length of 17 bytes for entries.
 */
public class EntryLastAccessed
{
    /**
     * Timestamp that indicates the last time entry was accessed (or,
     * for groups, last time any of entries was accessed).
     */
    public long lastAccessTime;

    /**
     * Timestamp that indicates creation time of content entry for which
     * last-accessed entry was last modified.
     * May be used for clean up purposes, to remove orphan entries.
     * Note that for group entries this just indicates one of many possible
     * creation times, so care has to be taken to consider if and how to use
     * it.
     */
    public long entryCreationTime;

    /**
     * Type of entry, from {@link LastAccessUpdateMethod#asByte}.
     */
    public byte type;

    public EntryLastAccessed(long accessTime, long createTime, byte type) {
        lastAccessTime = accessTime;
        entryCreationTime = createTime;
        this.type = type;
    }
    
    /*
    public EntryLastAccessed(StoredEntry<VKey> entry, long accessTime)
    {
        lastAccessTime = accessTime;
        entryCreationTime = entry.getCreationTime();
        type = entry.getLastAccessUpdateMethod().asByte();
    }

    public EntryLastAccessed(byte[] raw)
    {
        if (raw.length != 17) {
            throw new IllegalArgumentException("LastAccessed entry length must be 16 bytes, was: "+raw.length);
        }
        lastAccessTime = BDBConverters.getLongBE(raw, 0);
        entryCreationTime = BDBConverters.getLongBE(raw, 8);
        type = raw[16];
    }
    */

    public byte[] asBytes()
    {
        byte[] result = new byte[17];
        BDBConverters.putLongBE(result, 0, lastAccessTime);
        BDBConverters.putLongBE(result, 8, entryCreationTime);
        result[16] = type;
        return result;
    }
}
