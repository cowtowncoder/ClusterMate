package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.ByteUtil;
import com.fasterxml.storemate.shared.util.WithBytesCallback;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.store.EntryLastAccessed;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

public class StoredEntryConverterForTests
    extends StoredEntryConverter<TestKey, StoredEntry<TestKey>,FakeFullListItem>
{
    public final static byte METADATA_VERSION_1 = 0x11;
    
    public final static int OFFSET_VERSION = 0;
    public final static int OFFSET_LAST_ACCESS = 1;

    public final static int OFFSET_CREATE_TIME = 4;
    public final static int OFFSET_MIN_TTL = 12;
    public final static int OFFSET_MAX_TTL = 16;

    public final static int METADATA_LENGTH = 20;

    protected final TestKeyConverter _keyConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public StoredEntryConverterForTests(TestKeyConverter keyConverter) {
        _keyConverter = keyConverter;
    }

    /*
    /**********************************************************************
    /* Pass-through methods for key construction
    /**********************************************************************
     */

    @Override
    public TestKeyConverter keyConverter() {
        return _keyConverter;
    }
    
    /*
    /**********************************************************************
    /* Conversions for metadata section
    /**********************************************************************
     */
    
    /**
     * Method called to construct "custom metadata" section to be
     * used for constructing a new <code>Storable</code> instance.
     */
    @Override
    public ByteContainer createMetadata(long creationTime,
            byte lastAccessUpdateMethod, int minTTLSecs, int maxTTLSecs)
    {
        byte[] buffer = new byte[METADATA_LENGTH];
        buffer[OFFSET_VERSION] = METADATA_VERSION_1;
        buffer[OFFSET_LAST_ACCESS] = lastAccessUpdateMethod;
        _putLongBE(buffer, OFFSET_CREATE_TIME, creationTime);
        _putIntBE(buffer, OFFSET_MIN_TTL, minTTLSecs);
        _putIntBE(buffer, OFFSET_MAX_TTL, maxTTLSecs);
     
        return ByteContainer.simple(buffer, 0, METADATA_LENGTH);
    }

    /*
    /**********************************************************************
    /* Entry conversions
    /**********************************************************************
     */
    
    @Override
    public final StoredEntry<TestKey> entryFromStorable(final Storable raw) {
        return entryFromStorable(_key(raw.getKey()), raw);
    }

    @Override
    public final StoredEntry<TestKey> entryFromStorable(final TestKey key, final Storable raw)
    {
        return raw.withMetadata(new WithBytesCallback<StoredEntry<TestKey>>() {
            @Override
            public StoredEntry<TestKey> withBytes(byte[] buffer, int offset, int length) {
                return entryFromStorable(key, raw, buffer, offset, length);
            }
        });
    }

    @Override
    public StoredEntry<TestKey> entryFromStorable(TestKey key, Storable raw,
            byte[] buffer, int offset, int length)
    {
        int version = _extractVersion(key, buffer, offset, length);
        if (version != METADATA_VERSION_1) {
            _badData(key, "version 0x"+Integer.toHexString(version));
        }

        LastAccessUpdateMethod acc = _extractLastAccessUpdatedMethod(key, buffer, offset, length);
        final long creationTime = _extractCreationTime(buffer, offset, length);
        final int minTTLSecs = _extractMinTTLSecs(buffer, offset, length);
        final int maxTTLSecs = _extractMaxTTLSecs(buffer, offset, length);

        return new StoredEntryForTests(key, raw, creationTime, minTTLSecs, maxTTLSecs, acc);
    }

    @Override
    public ListItem minimalListItemFromStorable(Storable raw) {
        return defaultMinimalListItemFromStorable(raw);
    }

    @Override
    public FakeFullListItem fullListItemFromStorable(Storable raw) {
        return new FakeFullListItem(defaultMinimalListItemFromStorable(raw));
    }
    
    /*
    /**********************************************************************
    /* Other conversions
    /**********************************************************************
     */

    @Override
    public EntryLastAccessed createLastAccessed(StoredEntry<TestKey> entry, long accessTime)
    {
        return new EntryLastAccessed(accessTime, entry.calculateMaxExpirationTime(),
                 entry.getLastAccessUpdateMethod().asByte());
    }

    @Override
    public EntryLastAccessed createLastAccessed(byte[] raw) {
        return createLastAccessed(raw, 0, raw.length);
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
    
    /*
    /**********************************************************************
    /* Internal methods, data extraction
    /**********************************************************************
     */

    protected int _extractVersion(TestKey key, byte[] buffer, int offset, int length) {
        return buffer[offset+OFFSET_VERSION];
    }

    protected long _extractCreationTime(byte[] buffer, int offset, int length) {
        return _getLongBE(buffer, offset+OFFSET_CREATE_TIME);
    }

    protected int _extractMinTTLSecs(byte[] buffer, int offset, int length) {
        return _getIntBE(buffer, offset+OFFSET_MIN_TTL);
    }

    protected int _extractMaxTTLSecs(byte[] buffer, int offset, int length) {
        return _getIntBE(buffer, offset+OFFSET_MAX_TTL);
    }
    
    protected FakeLastAccess _extractLastAccessUpdatedMethod(TestKey key, byte[] buffer, int offset, int length)
    {
        int accCode = buffer[offset+OFFSET_LAST_ACCESS];
        FakeLastAccess acc = FakeLastAccess.valueOf(accCode);
        if (acc == null) {
            _badData(key, "invalid last-access-update-method 0x"+Integer.toHexString(accCode));
        }
        return acc;
    }

    private final static void _putLongBE(byte[] buffer, int offset, long value)
    {
        _putIntBE(buffer, offset, (int) (value >> 32));
        _putIntBE(buffer, offset+4, (int) value);
    }
    
    private final static void _putIntBE(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte) (value >> 24);
        buffer[++offset] = (byte) (value >> 16);
        buffer[++offset] = (byte) (value >> 8);
        buffer[++offset] = (byte) value;
    }

    private final static long _getLongBE(byte[] buffer, int offset)
    {
        long l1 = _getIntBE(buffer, offset);
        long l2 = _getIntBE(buffer, offset+4);
        return (l1 << 32) | ((l2 << 32) >>> 32);
    }
    
    private final static int _getIntBE(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24)
             | ((buffer[++offset] & 0xFF) << 16)
             | ((buffer[++offset] & 0xFF) << 8)
             | (buffer[++offset] & 0xFF)
             ;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    protected void _badData(final TestKey key, String msg) {
        throw new IllegalArgumentException("Bad metadata (key "+key+"): "+msg);
    }
    
    protected TestKey _key(StorableKey rawKey) {
        return _keyConverter.rawToEntryKey(rawKey);
    }
}
