package com.fasterxml.clustermate.service.store;

/**
 * Argument container used for limiting details of listing entries
 * (or names of entries).
 */
public class ListLimits
{
    // By default, list up to 1000 entries: callers must override
    private final static int DEFAULT_MAX_ENTRIES = 1000;

    // By default, allow listing to run for 15 seconds before quitting
    // (but note: must match at least one entry)
    private final static long DEFAULT_MAX_MSECS = 15000L;
    
    private final static ListLimits DEFAULT = new ListLimits(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_MSECS);
    
    private final int _maxEntries;
    
    private final long _maxMsecs;
    
    private ListLimits(int maxEntries, long maxMsecs)
    {
        _maxEntries = maxEntries;
        _maxMsecs = maxMsecs;
    }

    public static ListLimits defaultLimits() { return DEFAULT; }
    
    public ListLimits withMaxEntries(int newMax) {
        return (newMax == _maxEntries)  ? this : new ListLimits(newMax, _maxMsecs);
    }

    public ListLimits withMaxMsecs(long newMax) {
        return (newMax == _maxMsecs) ? this : new ListLimits(_maxEntries, newMax);
    }
    
    public int getMaxEntries() { return _maxEntries; }
    public long getMaxMsecs() { return _maxMsecs; }
}
