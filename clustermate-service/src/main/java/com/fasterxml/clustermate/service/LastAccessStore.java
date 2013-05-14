package com.fasterxml.clustermate.service;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.cfg.LastAccessConfig;
import com.fasterxml.clustermate.service.store.EntryLastAccessed;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Class that encapsulates optional storage of last-accessed
 * information, which implementations may choose to use for
 * things like dynamic expiration of not-recently-accessed entries.
 *<p>
 * Keys are derived from entry keys, so that grouped entries typically
 * map to a single entry, whereas individual entries just use
 * key as is or do not use last-accessed information at all.
 */
public abstract class LastAccessStore<K extends EntryKey, E extends StoredEntry<K>>
    implements StartAndStoppable
{
    protected final StoredEntryConverter<K,E,?> _entryConverter;

    /*
    /**********************************************************************
    /* Life cycle
    /**********************************************************************
     */

    public LastAccessStore(StoredEntryConverter<K,E,?> conv,
            LastAccessConfig config)
    {
        _entryConverter = conv;
    }

    /*
    /**********************************************************************
    /* StartAndStoppable dummy implementation
    /**********************************************************************
     */

    @Override
    public void start() { }

    @Override
    public abstract void prepareForStop();

    @Override
    public abstract void stop();

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */

    public long findLastAccessTime(E entry) {
        EntryLastAccessed acc = findLastAccessEntry(entry.getKey(), entry.getLastAccessUpdateMethod());
        return (acc == null) ? 0L : acc.lastAccessTime;
    }
    
    public long findLastAccessTime(K key, LastAccessUpdateMethod method)
    {
        EntryLastAccessed entry = findLastAccessEntry(key, method);
        return (entry == null) ? 0L : entry.lastAccessTime;
    }
    
    public EntryLastAccessed findLastAccessEntry(E entry) {
        return findLastAccessEntry(entry.getKey(), entry.getLastAccessUpdateMethod());
    }
    
    public abstract EntryLastAccessed findLastAccessEntry(K key, LastAccessUpdateMethod method);

    public abstract void updateLastAccess(E entry, long timestamp);

    /**
     * @return True if an entry was deleted; false otherwise (usually since there
     *    was no entry to delete)
     */
    public abstract boolean removeLastAccess(K key, LastAccessUpdateMethod method, long timestamp);
}
