package com.force.vagabond.server.jaxrs.testutil;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesAsUTF8String;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

/**
 * Value class that contains identifier used for accessing
 * piece of content: basically contains {@link PartitionId}
 * and external path.
 * Separate class is used to encapsulate details of hash calculation.
 */
public class TestKey
    extends EntryKey
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Main configuration
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Raw representation used for the underlying store
     */
    private final StorableKey _rawKey;
    
    private final PartitionId _partitionId;

    /**
     * Length of group id, in bytes (not necessarily characters).
     */
    private final int _namespaceLength;
    
    /**
     * Full path, including namespace, but not partition id.
     */
    private transient String _externalPath;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    protected TestKey(StorableKey raw,
            PartitionId partition, int groupPrefixLength)
    {
        _rawKey = raw;
        _partitionId = partition;
        _namespaceLength = groupPrefixLength;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors, converters
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public StorableKey asStorableKey() {
        return _rawKey;
    }

    @Override
    public byte[] asBytes() {
        return _rawKey.asBytes();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Extended API
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Accessor for getting full path, where groupId is the prefix, and local
     * part follows right after group id.
     */
    public String getExternalPath()
    {
        String str = _externalPath;
        if (str == null) {
            final int offset = TestKeyConverter.DEFAULT_KEY_HEADER_LENGTH;
            final int length = _rawKey.length() - offset;
            _externalPath = str = _rawKey.withRange(WithBytesAsUTF8String.instance, offset, length);
        }
        return str;
    }
    
    public PartitionId getPartitionId() { return _partitionId; }
    
    public String getGroupId()
    {
        if (_namespaceLength == 0) {
            return null;
        }
        return getExternalPath().substring(0, _namespaceLength);
    }

    public byte[] getNamespaceAsBytes()
    {
        if (_namespaceLength == 0) {
            return null;
        }
        return _rawKey.rangeAsBytes(TestKeyConverter.DEFAULT_KEY_HEADER_LENGTH, _namespaceLength);
    }

    /**
     * Callback-based accessor for accessing part of key formed when path itself
     * is dropped, and only partition and namespace are included.
     * Note that method can only be called when there is a group id; otherwise
     * a {@link IllegalStateException} will be thrown.
     */
    public <T> T withGroupPrefix(WithBytesCallback<T> cb)
    {
        if (_namespaceLength <= 0) {
            throw new IllegalStateException("Key does not have a group, can not call this method");
        }
        int len = TestKeyConverter.DEFAULT_KEY_HEADER_LENGTH + _namespaceLength;
        return _rawKey.withRange(cb, 0, len);
    }
    
    public boolean hasGroupId() {
        return (_namespaceLength > 0);
    }

    public int getGroupIdLength() {
        return _namespaceLength;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Overridden std methods
    ///////////////////////////////////////////////////////////////////////
     */
    
    @Override public boolean equals(Object o)
    {
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != getClass()) return false;
        TestKey other = (TestKey) o;
        // redundant, but faster than comparing byte arrays so:
        if (!other._partitionId.equals(_partitionId)) {
            return false;
        }
        return _rawKey.equals(other._rawKey);
    }

    @Override public int hashCode() {
    	return _partitionId.hashCode() ^ _rawKey.hashCode();
    }

    @Override public String toString() {
        if (_namespaceLength == 0) {
            return _partitionId.toString()+":0:"+getExternalPath();
        }
        return _partitionId.toString()+":"+_namespaceLength+":"+getExternalPath();
    }
}
