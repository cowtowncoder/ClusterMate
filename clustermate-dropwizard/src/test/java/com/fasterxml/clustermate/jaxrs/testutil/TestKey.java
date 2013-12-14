package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesAsUTF8String;

import com.fasterxml.clustermate.api.EntryKey;

/**
 * Value class that contains identifier used for accessing
 * piece of content: basically contains {@link CustomerId}
 * and external path.
 * Separate class is used to encapsulate details of hash calculation.
 */
public class TestKey extends EntryKey
{
    final static String TEST_KEY_PREFIX = "cm-test:";

    final static char TEST_KEY_SEPARATOR = '@';
    
    /*
    /**********************************************************************
    /* Main configuration
    /**********************************************************************
     */
    
    /**
     * Raw representation used for the underlying store
     */
    private final StorableKey _rawKey;

    private final CustomerId _customerId;
    
    /**
     * Full path, not including customer id.
     */
    private transient String _externalPath;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected TestKey(StorableKey raw, CustomerId customer)
    {
        _rawKey = raw;
        _customerId = customer;
    }
    
    /*
    /**********************************************************************
    /* Accessors, converters
    /**********************************************************************
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
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    /**
     * Accessor for getting full path, where partition id is the prefix, and local
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
    
    public CustomerId getCustomerId() { return _customerId; }
    
    /*
    /**********************************************************************
    /* Overridden std methods
    /**********************************************************************
     */
    
    @Override public boolean equals(Object o)
    {
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != getClass()) return false;
        TestKey other = (TestKey) o;
        // redundant, but faster than comparing byte arrays so:
        if (!other._customerId.equals(_customerId)) {
            return false;
        }
        return _rawKey.equals(other._rawKey);
    }

    @Override public int hashCode() {
        return _customerId.hashCode() ^ _rawKey.hashCode();
    }

    @Override public String toString()
    {
        return _customerId.toString()+TEST_KEY_SEPARATOR+getExternalPath();
    }
}
