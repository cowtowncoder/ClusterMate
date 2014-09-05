package com.fasterxml.clustermate.jaxrs.testutil;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.backend.bdbje.BDBLastAccessStoreImpl;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessConfig;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.clustermate.service.store.StoresImpl;

public class StoresForTests extends StoresImpl<TestKey, StoredEntry<TestKey>>
{
    private final LastAccessConfig _lastAccessConfig;
    private final File _dbRootForLastAccess;
    private Environment _lastAccessEnv;
    private LastAccessStore<TestKey, StoredEntry<TestKey>,LastAccessUpdateMethod> _lastAccessStore;
    
    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>,FakeFullListItem> entryFactory,
            StorableStore entryStore, NodeStateStore<IpAndPort, ActiveNodeState> nodeStates) {
        this(config, timeMaster, jsonMapper,
                entryFactory, entryStore, nodeStates, null);
    }

    public StoresForTests(ServiceConfig config, TimeMaster timeMaster, ObjectMapper jsonMapper,
            StoredEntryConverter<TestKey, StoredEntry<TestKey>,FakeFullListItem> entryConverter,
            StorableStore entryStore, NodeStateStore<IpAndPort, ActiveNodeState> nodeStates,
            File dataStoreRoot)
    {
        // null -> no need for remote node state store
        super(config, timeMaster, jsonMapper, entryConverter, entryStore, nodeStates, null, dataStoreRoot);
        if (dataStoreRoot == null) {
            dataStoreRoot = config.metadataDirectory;
        }
        _dbRootForLastAccess = new File(dataStoreRoot, "lastAccess");        
        _lastAccessConfig = config.lastAccess;
    }

    /*
    /**********************************************************************
    /* Start/stop life-cycle
    /**********************************************************************
     */

    @Override
    protected boolean _openLocalStores(boolean log, boolean allowCreate, boolean writeAccess)
    {
        setInitProblem(null);
        final String logPrefix = allowCreate ? "Trying to open (or initialize)" : "Trying to open";

        // first things first: must have directories for Environment
        if (!_verifyOrCreateDirectory(_dbRootForLastAccess, log)) {
            return false;
        }
        
        // then last access store:
        if (log) {
            LOG.info(logPrefix+" Last-access store...");
        }
        _lastAccessEnv = new Environment(_dbRootForLastAccess,
                lastAccessEnvConfig(allowCreate, writeAccess));
        try {
            _lastAccessStore = buildAccessStore(_lastAccessEnv, _lastAccessConfig);
        } catch (Exception e) {
            String prob = "Failed to open Last-access store: ("+e.getClass().getName()+") "+e.getMessage();
            setInitProblem(prob);
            throw new IllegalStateException(prob, e);
        }
        if (_lastAccessStore != null) {
            _lastAccessStore.start();
        }
        if (log) {
            LOG.info("Last-access store succesfully opened");
        }
        return true;
    }

    @Override
    protected void _prepareToCloseLocalStores() {
        _lastAccessStore.prepareForStop();
    }
    
    @Override
    protected void _closeLocalStores() {
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
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Store accessors
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public LastAccessStore<TestKey, StoredEntry<TestKey>,LastAccessUpdateMethod> getLastAccessStore() {
        return _lastAccessStore;
    }

    // Not needed for tests, leave stubbed out
    @Override
    public NodeStateStore<IpAndPort, ActiveNodeState> getRemoteNodeStore() {
        return null;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Other
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected LastAccessStore<TestKey, StoredEntry<TestKey>,LastAccessUpdateMethod> buildAccessStore(Environment env,
            LastAccessConfig config)
    {
        LastAccessStore<TestKey, StoredEntry<TestKey>,LastAccessUpdateMethod> lastAccessStore
            = new BDBLastAccessStoreImpl<TestKey, StoredEntry<TestKey>,LastAccessUpdateMethod>(config,
                    new LastAccessConverterForTests(),
                    env);
        return lastAccessStore;
    }

    protected EnvironmentConfig lastAccessEnvConfig(boolean allowCreate, boolean writeAccess)
    {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(allowCreate);
        config.setReadOnly(!writeAccess);
        config.setSharedCache(false);
        config.setCacheSize(_lastAccessConfig.cacheSize.getNumberOfBytes());
        config.setLockTimeout(_lastAccessConfig.lockTimeoutMsecs, TimeUnit.MILLISECONDS);
        return config;
    }

    protected void _verifyDirectory(File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalStateException("Local database path '"+dir.getAbsolutePath()
                    +"' does not point to a directory; can not open local store -- read Documentation on how to initialize a node!");
        }
    }
    
    protected boolean _verifyOrCreateDirectory(File dir, boolean logInfo) {
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                LOG.error("There is file {} which is not directory: CAN NOT create local database!",
                        dir.getAbsolutePath());
                return false;
            }
            LOG.info("Directory {} exists, will use it", dir.getAbsolutePath());
        } else {
            LOG.info("Directory {} does not exist, will try to create", dir.getAbsolutePath());
            if (!dir.mkdirs()) {
                LOG.error("FAILed to create directory {}: CAN NOT create local database!",
                        dir.getAbsolutePath());
                return false;
            }
            if (logInfo) {
                LOG.info("Directory succesfully created");
            }
        }
        return true;
    }
}
