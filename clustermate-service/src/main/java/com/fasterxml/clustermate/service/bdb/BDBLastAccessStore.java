package com.fasterxml.clustermate.service.bdb;

import com.sleepycat.je.*;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.LastAccessStore;
import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.cfg.LastAccessConfig;
import com.fasterxml.clustermate.service.store.EntryLastAccessed;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Intermediate base class for BDB-JE - backed {@link LastAccessStore}
 * implementation.
 */
public abstract class BDBLastAccessStore<K extends EntryKey, E extends StoredEntry<K>>
    extends LastAccessStore<K, E>
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

    /*
    /**********************************************************************
    /* Life cycle
    /**********************************************************************
     */

    public BDBLastAccessStore(Environment env, StoredEntryConverter<K,E,?> conv,
            LastAccessConfig config)
        throws DatabaseException
    {
        super(conv, config);
        _store = env.openDatabase(null, // no TX
                "LastAccessed", dbConfig(env, config));
    }

    @Override
    public void prepareForStop()
    {
        // 02-May-2013, tsaloranta: Better sync() if we use deferred writes
        //   (otherwise not allowed to)
        if (_store.getConfig().getDeferredWrite()) {
            _store.sync();
        }
    }
    
    @Override
    public void stop() {
        _store.close();
    }

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */

    @Override
    public EntryLastAccessed findLastAccessEntry(K key, LastAccessUpdateMethod method)
    {
        DatabaseEntry lastAccessKey = lastAccessKey(key, method);
        if (lastAccessKey == null) {
            return null;
        }
        DatabaseEntry entry = new DatabaseEntry();
        OperationStatus status = _store.get(null, lastAccessKey, entry, null);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return _entryConverter.createLastAccessed(entry.getData(), entry.getOffset(), entry.getSize());
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return null;
    }

    @Override
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

    @Override
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

    protected DatabaseConfig dbConfig(Environment env, LastAccessConfig config)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig econfig = env.getConfig();
        dbConfig.setReadOnly(econfig.getReadOnly());
        dbConfig.setAllowCreate(econfig.getAllowCreate());
        dbConfig.setSortedDuplicates(false);
        dbConfig.setDeferredWrite(config.useDeferredWrites());
        return dbConfig;
    }
    private DatabaseEntry lastAccessKey(E entry) {
        return lastAccessKey(entry.getKey(), entry.getLastAccessUpdateMethod());
    }
    
    protected abstract DatabaseEntry lastAccessKey(K key, LastAccessUpdateMethod acc);
}
