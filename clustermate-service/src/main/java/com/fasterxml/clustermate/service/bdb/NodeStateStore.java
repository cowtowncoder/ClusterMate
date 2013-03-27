package com.fasterxml.clustermate.service.bdb;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.clustermate.service.cluster.ActiveNodeState;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.UTF8Encoder;

/**
 * Class that handles operations on the BDB-backed Node state
 * table (BDB database) and related indexes.
 * This store is used for persisting enough state regarding
 * peer-to-peer operations to reduce amount of re-synchronization
 * needed when node instances are restarted.
 */
public class NodeStateStore implements StartAndStoppable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    
    protected final ObjectReader _jsonReader;
    protected final ObjectWriter _jsonWriter;

    /**
     * Underlying BDB entity store ("table")
     * for storing node states.
     */
    protected final Database _store;
        
    /*
    /**********************************************************************
    /* Life cycle
    /**********************************************************************
     */
        
    public NodeStateStore(Environment env, ObjectMapper jsonMapper) throws DatabaseException
    {
        _jsonReader = jsonMapper.reader(ActiveNodeState.class);
        _jsonWriter = jsonMapper.writerWithType(ActiveNodeState.class);
        _store = env.openDatabase(null, // no TX
                "Nodes", dbConfig(env));
    }

    public void start() { }
    
    public void prepareForStop() {
        /* 27-Mar-2013, tatu: Not much we can do; sync() only needed when
         *   using deferred writes.
         */
//        _store.sync();
    }
    
    public void stop() {
        _store.close();
    }

    /*
    /**********************************************************************
    /* Content lookups
    /**********************************************************************
     */

    /**
     * Method that can be used to find specified entry, without updating
     * its last-accessed timestamp.
     */
    public ActiveNodeState findEntry(IpAndPort key) throws IOException
    {
        DatabaseEntry dbKey = _key(key);
        if (dbKey == null) {
            return null;
        }
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status = _store.get(null, dbKey, data, null);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return _fromDB(data);
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return null;
    }

    /**
     * Method for simply reading all node entries store has; called usually
     * only during bootstrapping.
     */
    public List<ActiveNodeState> readAll(KeySpace keyspace)
    {
        List<ActiveNodeState> all = new ArrayList<ActiveNodeState>(30);
        CursorConfig config = new CursorConfig();
        Cursor crsr = _store.openCursor(null, config);
        final DatabaseEntry keyEntry;
        final DatabaseEntry data = new DatabaseEntry();

        keyEntry = new DatabaseEntry();
        OperationStatus status = crsr.getFirst(keyEntry, data, null);
        try {
            int index = 0;

            for (; status == OperationStatus.SUCCESS; status = crsr.getNext(keyEntry, data, null)) {
                try {
                    all.add(_fromDB(data));
                } catch (Exception e) {
                    String key = "N/A";
                    try {
                        IpAndPort ip = _keyFromDB(keyEntry);
                        key = ip.toString();
                    } catch (Exception e2) {
                        key = "[Corrupt Key]";
                    }
                    LOG.error("Invalid Node state entry in BDB, entry #{}, key '{}', skipping. Problem ({}): {}",
                            new Object[] { index, key, e.getClass().getName(), e.getMessage()});
                }
                ++index;
            }
        } finally {
            crsr.close();
        }
        return all;
    }
    
    /*
    /**********************************************************************
    /* Content modification
    /**********************************************************************
     */
    
    public void upsertEntry(ActiveNodeState entry) throws IOException
    {
        _store.put(null, _key(entry.getAddress()), _toDB(entry));
    }

    public boolean deleteEntry(IpAndPort key) {
        return deleteEntry(key.toString());
    }

    public boolean deleteEntry(String keyStr) {
        DatabaseEntry lastAccessKey = _key(keyStr);
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

    protected DatabaseEntry _key(IpAndPort key) {
        return _key(key.toString());
    }

    protected DatabaseEntry _key(String key) {
        return new DatabaseEntry(UTF8Encoder.encodeAsUTF8(key));
    }

    protected IpAndPort _keyFromDB(DatabaseEntry entry) {
        return new IpAndPort(UTF8Encoder.decodeFromUTF8(entry.getData(),
                entry.getOffset(), entry.getSize()));
    }
    
    protected DatabaseEntry _toDB(ActiveNodeState state) throws IOException {
        return new DatabaseEntry(_jsonWriter.writeValueAsBytes(state));
    }

    protected ActiveNodeState _fromDB(DatabaseEntry entry) throws IOException {
        return _jsonReader.readValue(entry.getData(), entry.getOffset(), entry.getSize());
    }
}
