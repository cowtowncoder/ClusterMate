package com.fasterxml.clustermate.service.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.StartAndStoppable;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.bdb.LastAccessStore;
import com.fasterxml.clustermate.service.bdb.NodeStateStore;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;

public abstract class StoresImpl<K extends EntryKey, E extends StoredEntry<K>>
	extends Stores<K, E>
     implements StartAndStoppable
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * For Node stores we do not really need much any caching; but
     * give half a meg just to keep branch nodes in memory.
     */
    private final static long NODE_BDB_CACHE_SIZE = 512L * 1024L;

    /**
     * Last-accessed can benefit more from cache, give it 20 megs for now
     */
    private final static long LAST_ACCESS_BDB_CACHE_SIZE = 20L * 1024L * 1024L;

    /**
     * Last-access table may get rather high concurrency as it may be
     * updated for every GET request, so let's boost from 1 to reasonably
     * high prime number.
     */
    protected final static int DEFAULT_LAST_ACCESS_LOCK_TABLES = 17;

    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */

    protected final TimeMaster _timeMaster;

    protected final ObjectMapper _jsonMapper;
    
    /**
     * Directory that contains environment used for storing state
     * information for this node instance and its peers.
     * Separate from other storage to reduce likelihood of corruption;
     * handling of node data is fully transactional as it should
     * be less likely to corrupt, ever.
     */
    private final File _bdbRootForNodes;

    /**
     * Directory for environment used for storing last-accessed
     * information.
     */
    private final File _bdbRootForLastAccess;

    /*
    /**********************************************************************
    /* Stores
    /**********************************************************************
     */
    
    /**
     * Separately managed {@link StorableStore} that handles actual entry
     * storage details.
     */
    protected final StorableStore _entryStore;

    /**
     * We also need a factory for converting keys, entries.
     */
    protected final StoredEntryConverter<K,E,?> _entryConverter;
    
    // Separate Environments for node state, last-accessed: latter much bigger,
    // former possibly transactional
    
    private Environment _nodeEnv;

    private NodeStateStore _nodeStore;

    private Environment _lastAccessEnv;

    private LastAccessStore<K, E> _lastAccessStore;

    /*
    /**********************************************************************
    /* Status
    /**********************************************************************
     */

    /**
     * Marker flag used to indicate whether this store is currently active
     * and able to process things.
     */
    protected final AtomicBoolean _active = new AtomicBoolean(false);

    /**
     * Error message used to indicate why initialization failed, if it did.
     */
    protected volatile String _initProblem;

    /*
    /**********************************************************************
    /* Basic life-cycle
    /**********************************************************************
     */
     
    public StoresImpl(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<K,E,?> entryConverter,
            StorableStore entryStore, File bdbEnvRoot)
    {
        _timeMaster = timeMaster;
        _jsonMapper = jsonMapper;
        _entryConverter = entryConverter;
        _entryStore = entryStore;
        if (bdbEnvRoot == null) {
            bdbEnvRoot = config.metadataDirectory;
        }
        _bdbRootForNodes = new File(bdbEnvRoot, "nodes");
        _bdbRootForLastAccess = new File(bdbEnvRoot, "lastAccess");        
    }

    @Override
    public void start() throws IOException {
        // nothing much to do here; we actually force init on construction
    }

    @Override
    public void prepareForStop()
    {
        /* Nothing urgent we have to do, but let's let
         * stores know in case they want to do some flushing
         * ahead of time
         * 
         */
        if (_nodeStore != null) {
            try {
                _nodeStore.prepareForStop();
            } catch (Exception e) {
                LOG.warn("Problems with prepareForStop() on nodeStore", e);
            }
        }
        if (_entryStore != null) {
            try {
                _entryStore.prepareForStop();
            } catch (Exception e) {
                LOG.warn("Problems with prepareForStop() on entryStore", e);
            }
        }
        if (_lastAccessStore != null) {
            try {
                _lastAccessStore.prepareForStop();
            } catch (Exception e) {
                LOG.warn("Problems with prepareForStop() on lastAccessStore", e);
            }
        }
    }
    
    @Override
    public void stop() throws IOException
    {
        _active.set(false);
        // close node store first, more important to preserve:
        if (_nodeStore == null) {
            LOG.warn("Odd: Node store not open? Skipping");
        } else {
            LOG.info("Closing Node store...");
            try {
                _nodeStore.stop();
                LOG.info("Closing Node store environment...");
                _nodeEnv.close();
            } catch (Exception e) {
                LOG.error("Problems closing node store: {}", e.getMessage(), e);
            }
        }

        // then entry metadata
        if (_entryStore == null) {
            LOG.warn("Odd: Entry Metadata store not open? Skipping");
        } else {
            LOG.info("Closing Entry metadata store...");
            try {
                _entryStore.stop();
            } catch (Exception e) {
                LOG.error("Problems closing Entry Metadata store: {}", e.getMessage(), e);
            }
        }

        // and finally, last-accessed (most disposable)
        if (_lastAccessStore == null) {
            LOG.warn("Odd: Last-access store not open? Skipping");
        } else {
            LOG.info("Closing Last-access store...");
            try {
                _lastAccessStore.stop();
                LOG.info("Closing Last-access environment...");
                _lastAccessEnv.close();
            } catch (Exception e) {
                LOG.error("Problems closing Last-access store: {}", e.getMessage(), e);
            }
        }
        
        LOG.info("BDB data stores and environments closed");
    }
    
    /*
    /**********************************************************************
    /* Explicit initialization, varies for different use cases
    /**********************************************************************
     */

    /**
     * Method called to forcibly initialize environment as configured,
     * and then open it normally.
     */
    public boolean initAndOpen(boolean logInfo)
    {
        // first things first: must have directories for Environment
        if (!_verifyOrCreateDirectory(_bdbRootForNodes, logInfo)
               || !_verifyOrCreateDirectory(_bdbRootForLastAccess, logInfo)) {
            return false;
        }
        _openBDBs(true, true, true);
        _active.set(true);
        return true;
    }
    
    /**
     * Method called to open BDB stores if they exist, in read/write mode.
     */
    public void openIfExists()
    {
        // First: verify that the dirs exist:
        _verifyDirectory(_bdbRootForNodes);
        // then try opening
        _openBDBs(true, false, true);
        _active.set(true);
    }

    /**
     * Method called to open BDB stores if they exist, and only open for reading.
     */
    public void openForReading(boolean log)
    {
        // First: verify that the dirs exist:
        _verifyDirectory(_bdbRootForNodes);
        // then try opening
        _openBDBs(log, false, false);
        _active.set(true);
    }
    
    protected void _openBDBs(boolean log,
          boolean allowCreate, boolean writeAccess)
    {
        _initProblem = null;

        final String logPrefix = allowCreate ? "Trying to open (or initialize)" : "Trying to open";

        // First, node (state) store
        if (log) {
            LOG.info(logPrefix+" Node store...");
        }
        _nodeEnv = new Environment(_bdbRootForNodes, nodeEnvConfig(allowCreate, writeAccess));
        try {
            _nodeStore = new NodeStateStore(_nodeEnv, _jsonMapper);
        } catch (DatabaseException e) {
            _initProblem = "Failed to open Node store: "+e.getMessage();
            throw new IllegalStateException(_initProblem, e);
        }
        if (log) {
            LOG.info("Node store succesfully opened");
        }

        // then last access store:
        if (log) {
            LOG.info(logPrefix+" Last-access store...");
        }
        _lastAccessEnv = new Environment(_bdbRootForLastAccess, lastAccessEnvConfig(allowCreate, writeAccess));
        try {
            _lastAccessStore = buildAccessStore(_lastAccessEnv);
        } catch (DatabaseException e) {
            _initProblem = "Failed to open Last-access store: "+e.getMessage();
            throw new IllegalStateException(_initProblem, e);
        }
        if (log) {
            LOG.info("Last-access store succesfully opened");
        }
    }

    protected abstract LastAccessStore<K,E> buildAccessStore(Environment env);
    
    /*
    /**********************************************************************
    /* Simple accessors
    /**********************************************************************
     */

    @Override
    public boolean isActive() { return _active.get(); }

    @Override
    public String getInitProblem() { return _initProblem; }

    @Override
    public File getNodeDirectory() { return _bdbRootForNodes; }

    @Override
    public StoredEntryConverter<K,E,?> getEntryConverter() { return _entryConverter; }
    
    @Override
    public StorableStore getEntryStore() { return _entryStore; }
    @Override
    public NodeStateStore getNodeStore() { return _nodeStore; }
    @Override
    public LastAccessStore<K, E> getLastAccessStore() { return _lastAccessStore; }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected void _verifyDirectory(File dir)
    {
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalStateException("BDB path '"+dir.getAbsolutePath()
                    +"' does not point to a directory; can not open BDB -- read Documentation on how to 'init' a node!"
                    +" (usually something like './command.sh init')");
        }
    }
    
    protected boolean _verifyOrCreateDirectory(File dir, boolean logInfo)
    {
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                LOG.error("There is file {} which is not directory: CAN NOT create BDB Environment!",
                        dir.getAbsolutePath());
                return false;
            }
            LOG.info("Directory {} exists, will use it", dir.getAbsolutePath());
        } else {
            LOG.info("Directory {} does not exist, will try to create", dir.getAbsolutePath());
            if (!dir.mkdirs()) {
                LOG.error("FAILed to create directory {}: CAN NOT create BDB Environment!",
                        dir.getAbsolutePath());
                return false;
            }
            if (logInfo) {
                LOG.info("Directory succesfully created");
            }
        }
        return true;
    }

    protected EnvironmentConfig nodeEnvConfig(boolean allowCreate, boolean writeAccess)
    {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(allowCreate);
        config.setReadOnly(!writeAccess);
        config.setSharedCache(false);
        config.setCacheSize(NODE_BDB_CACHE_SIZE);
        config.setDurability(Durability.COMMIT_SYNC);
        // default of 500 msec too low:
        config.setLockTimeout(5000L, TimeUnit.MILLISECONDS);
        return config;
    }
    
    protected EnvironmentConfig lastAccessEnvConfig(boolean allowCreate, boolean writeAccess)
    {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(allowCreate);
        config.setReadOnly(!writeAccess);
        config.setSharedCache(false);
        config.setCacheSize(LAST_ACCESS_BDB_CACHE_SIZE);
        // default of 500 msec too low:
        config.setLockTimeout(5000L, TimeUnit.MILLISECONDS);
        // and to get decent concurrency, default of 1 won't do:
        config.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, String.valueOf(DEFAULT_LAST_ACCESS_LOCK_TABLES));
        return config;
    }
}
