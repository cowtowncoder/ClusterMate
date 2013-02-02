package com.fasterxml.clustermate.service.bdb;

import com.sleepycat.je.*;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.store.EntryLastAccessed;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Class that encapsulates BDB-JE backed storage of last-accessed
 * information.
 *<p>
 * Keys are derived from entry keys, so that grouped entries typically
 * map to a single entry, whereas individual entries just use
 * key as is or do not use last-accessed information at all.
 *<p>
 * Note that a concrete implementation is required due to details
 * of actual key being used.
 */
public abstract class LastAccessStore<K extends EntryKey, E extends StoredEntry<K>>
{
    /*
    /**********************************************************************
    /* BDB store for last-accessed timestamps
    /**********************************************************************
     */

    /**
     * Underlying BDB entity store ("table") for last-accessed timestamps
     */
    protected final Database _store;

    protected final StoredEntryConverter<K,E,?> _entryConverter;
    
    /*
    /**********************************************************************
    /* Life cycle
    /**********************************************************************
     */

    public LastAccessStore(Environment env, StoredEntryConverter<K,E,?> conv)
        throws DatabaseException
    {
        _store = env.openDatabase(null, // no TX
                "LastAccessed", dbConfig(env));
        _entryConverter = conv;
    }

    public void stop()
    {
        _store.close();
    }

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
    
    public EntryLastAccessed findLastAccessEntry(K key, LastAccessUpdateMethod method)
    {
        DatabaseEntry lastAccessKey = lastAccessKey(key, method);
        if (lastAccessKey == null) {
            return null;
        }
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status = _store.get(null, lastAccessKey, data, null);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return _entryConverter.createLastAccessed(data.getData());
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return null;
    }

    public void updateLastAccess(E entry, long timestamp)
    {
        DatabaseEntry lastAccessKey = lastAccessKey(entry);
        if (lastAccessKey != null) {
            /* 18-Sep-2012, tatu: Should we try to enforce constraint on monotonically
             *   increasing timestamps? Since this is not used for peer-to-peer syncing,
             *   minor deviations from exact value are ok (deletion occurs after hours,
             *   or at most minutes since last access), so let's avoid extra lookup.
             *   Same goes for other settings
             */
            EntryLastAccessed acc = _entryConverter.createLastAccessed(entry, timestamp);
            _store.put(null, lastAccessKey, new DatabaseEntry(acc.asBytes()));
        }
    }

    /**
     * @return True if an entry was deleted; false otherwise (usually since there
     *    was no entry to delete)
     */
    public boolean removeLastAccess(K key, LastAccessUpdateMethod method, long timestamp)
    {
        DatabaseEntry lastAccessKey = lastAccessKey(key, method);
        if (lastAccessKey != null) {
            OperationStatus status = _store.delete(null, lastAccessKey);
            switch (status) {
            case SUCCESS:
            case KEYEXIST:
                return true;
            default:
            }
        }
        return false;
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected DatabaseConfig dbConfig(Environment env)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig econfig = env.getConfig();
        dbConfig.setReadOnly(econfig.getReadOnly());
        dbConfig.setAllowCreate(econfig.getAllowCreate());
        dbConfig.setSortedDuplicates(false);
        return dbConfig;
    }

    private DatabaseEntry lastAccessKey(E entry) {
        return lastAccessKey(entry.getKey(), entry.getLastAccessUpdateMethod());
    }
    
    protected abstract DatabaseEntry lastAccessKey(K key, LastAccessUpdateMethod acc);
}
