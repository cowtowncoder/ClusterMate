package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

public enum FakeLastAccess implements LastAccessUpdateMethod
{
    NONE(1), GROUPED(2), INDIVIDUAL(3);
    
    private final int _index;

    private FakeLastAccess(int index) {
        _index = index;
    }

    @Override public int asInt() { return _index; }
    @Override public byte asByte() { return (byte) _index; }
    @Override public boolean meansNoUpdate() { return (this == NONE); }

    public static FakeLastAccess valueOf(int v)
    {
        if (v == NONE._index) {
            return NONE;
        }
        if (v == INDIVIDUAL._index) {
            return INDIVIDUAL;
        }
        if (v == GROUPED._index) {
            return GROUPED;
        }
        return null;
    }
}
