package com.fasterxml.clustermate.service.bdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.UTF8Encoder;

import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.service.NodeStateStore;
import com.fasterxml.clustermate.service.cluster.ActiveNodeState;

/**
 * Concrete {@link NodeStateStore} implementation that uses BDB-JE
 * as the backing storage.
 */
public class BDBNodeStateStore extends NodeStateStore
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
        
    public BDBNodeStateStore(Environment env, ObjectMapper jsonMapper) throws DatabaseException
    {
        _jsonReader = jsonMapper.reader(ActiveNodeState.class);
        _jsonWriter = jsonMapper.writerWithType(ActiveNodeState.class);
        _store = env.openDatabase(null, // no TX
                "Nodes", dbConfig(env));
    }

    /*
    /**********************************************************************
    /* StartAndStoppable dummy implementation
    /**********************************************************************
     */

    @Override
    public void start() { }
    @Override
    public void prepareForStop() {
        /* 27-Mar-2013, tatu: Not much we can do; sync() only needed when
         *   using deferred writes.
         */
//        _store.sync();
    }
    
    @Override
    public void stop() {
        _store.close();
    }

    /*
    /**********************************************************************
    /* Public API: Content lookups
    /**********************************************************************
     */

    @Override
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

    @Override
    public List<ActiveNodeState> readAll()
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
    /* Public API: Content modification
    /**********************************************************************
     */

    @Override
    public void upsertEntry(ActiveNodeState entry) throws IOException
    {
        _store.put(null, _key(entry.getAddress()), _toDB(entry));
    }

    @Override
    public boolean deleteEntry(String keyStr)
    {
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
