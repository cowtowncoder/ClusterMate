package com.fasterxml.clustermate.service;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.cfg.LastAccessConfig;
import com.fasterxml.clustermate.service.store.EntryLastAccessed;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.BackendStatsConfig;
import com.fasterxml.storemate.store.backend.IterationAction;

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
    /* Public API, metadata
    /**********************************************************************
     */

    /**
     * Method for checking whether link {@link #getEntryCount} has a method
     * to produce entry count using a method that is more efficient than
     * explicitly iterating over entries.
     * Note that even if true is returned, some amount of iteration may be
     * required, and operation may still be more expensive than per-entry access.
     */
    public abstract boolean hasEfficientEntryCount();

    /**
     * Accessor for getting approximate count of entries in the underlying
     * main entry database,
     * if (but only if) it can be accessed in
     * constant time without actually iterating over data.
     */
    public abstract long getEntryCount();

    /**
     * Accessor for backend-specific statistics information regarding
     * primary entry storage.
     * 
     * @param config Settings to use for collecting statistics
     */
    public abstract Object getEntryStatistics(BackendStatsConfig config);

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

    /**
     * Method called to update last-accessed information for given entry.
     * 
     * @param timestamp Actual last-accessed value
     */
    public abstract void updateLastAccess(E entry, long timestamp);

    /**
     * @return True if an entry was deleted; false otherwise (usually since there
     *    was no entry to delete)
     */
    public abstract boolean removeLastAccess(K key, LastAccessUpdateMethod method, long timestamp);

    /*
    /**********************************************************************
    /* Helper classes for iteration (mostly to support cleanup)
    /**********************************************************************
     */

    /**
     * Callback for safe traversal over last-accessed entries
     */
    public abstract class StorableIterationCallback
    {
        /**
         * Method called for each entry, to check whether entry with the key
         * is to be processed.
         * 
         * @return Action to take for the entry with specified key
         */
        public abstract IterationAction verifyKey(StorableKey key);

        /**
         * Method called for each "accepted" entry (entry for which
         * {@link #verifyKey} returned {@link IterationAction#PROCESS_ENTRY}).
         * 
         * @return Action to take; specifically, whether to continue processing
         *   or not (semantics for other values depend on context)
         */
        public abstract IterationAction processEntry(Storable entry)
            throws StoreException;
    }
}
