package com.fasterxml.clustermate.service;

/**
 * Enumeration that defines method used for figuring out how last-access
 * information is stored.
 */
public enum LastAccessUpdateMethod
{
    /**
     * Value that means that no last-accessed information is maintained.
     * Used by clients that do not need this information, as expiration is
     * based solely on time entry lives and not on access patterns.
     * Has the benefit of lower overhead, as last-accessed information need
     * not be maintained or consulted during deletions.
     */
    NONE(1),

    /**
     * Value that means that the last-accessed information is maintained separately
     * for the entry itself, and not shared with any other. This means that the
     * full content id is used as they key for last-accessed entry.
     *<p>
     * This is the most costly of choices, due to number of entries.
     */
    INDIVIDUAL(2),

    /**
     * Value that means that the last-accessed information is maintained for
     * a group of entries, as defined by their shared routing prefix of
     * content id. 
     *<p>
     * This value has moderate overhead: last-accessed information is maintained
     * same way as with {@link #INDIVIDUAL}, but number of entries may be lower
     * due to sharing of that entry.
     */
    GROUPED(3)
    ;

    protected final int _value;
    protected final byte _valueAsByte;
    
    private LastAccessUpdateMethod(int v)
    {
        _value = v;
        _valueAsByte = (byte) v;
    }

    public int asInt() { return _value; }
    public byte asByte() { return _valueAsByte; }
    
    public static LastAccessUpdateMethod valueOf(int v)
    {
        if (v == NONE._value) {
            return NONE;
        }
        if (v == INDIVIDUAL._value) {
            return INDIVIDUAL;
        }
        if (v == GROUPED._value) {
            return GROUPED;
        }
        return null;
    }
}
