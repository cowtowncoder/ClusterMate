package com.fasterxml.clustermate.service.lastaccess;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.storemate.shared.util.ByteUtil;
import com.fasterxml.storemate.store.lastaccess.EntryLastAccessed;
import com.fasterxml.storemate.store.lastaccess.LastAccessConverter;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

/**
 * Partial {@link LastAccessConverter} implementation that simplifies
 * actual implementations.
 */
public abstract class LastAccessConverterBase<K extends EntryKey, E extends StoredEntry<K>>
    extends LastAccessConverter<K,E,LastAccessUpdateMethod>
{
    @Override
    public EntryLastAccessed createLastAccessed(E entry, long accessTime)
    {
        return new EntryLastAccessed(accessTime, entry.calculateMaxExpirationTime(),
                entry.getLastAccessUpdateMethod().asByte());
    }

    @Override
    public EntryLastAccessed createLastAccessed(byte[] raw, int offset, int length)
    {
        if (length != 17) {
            throw new IllegalArgumentException("LastAccessed entry length must be 17 bytes, was: "+length);
        }
        long accessTime = ByteUtil.getLongBE(raw, offset);
        long expirationTime = ByteUtil.getLongBE(raw, offset+8);
        byte type = raw[16];
        return new EntryLastAccessed(accessTime, expirationTime, type);
    }

    @Override
    public byte[] createLastAccessedKey(E entry) {
        return createLastAccessedKey(entry.getKey(), entry.getLastAccessUpdateMethod());
    }
    
    @Override
    public abstract byte[] createLastAccessedKey(K key, LastAccessUpdateMethod method);
}
