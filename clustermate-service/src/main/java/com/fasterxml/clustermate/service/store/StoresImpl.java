package com.fasterxml.clustermate.service.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.storemate.store.state.NodeStateStore;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.state.ActiveNodeState;

public abstract class StoresImpl<K extends EntryKey, E extends StoredEntry<K>>
	extends Stores<K, E>
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */

    protected final TimeMaster _timeMaster;

    protected final ObjectMapper _jsonMapper;

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

    /**
     * Store for keeping track of states of local peers.
     */
    private final NodeStateStore<IpAndPort, ActiveNodeState> _nodeStore;

    /**
     * Store for keeping minimal state information about nodes of the remote
     * cluster (if configured).
     */
    private final NodeStateStore<IpAndPort, ActiveNodeState> _remoteNodeStore;
    
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
            StorableStore entryStore,
            NodeStateStore<IpAndPort, ActiveNodeState> nodeStates,
            NodeStateStore<IpAndPort, ActiveNodeState> remoteNodeStates,
            File dbEnvRoot)
    {
        _timeMaster = timeMaster;
        _jsonMapper = jsonMapper;
        _entryConverter = entryConverter;
        _entryStore = entryStore;
        _nodeStore = nodeStates;
        _remoteNodeStore = remoteNodeStates;
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

        try {
            _prepareToCloseLocalStores();
        } catch (Exception e) {
            LOG.warn("Problems calling _prepareToCloseLocalStores()", e);
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

        // And then possibly other local database
        try {
            _closeLocalStores();
        } catch (Exception e) {
            LOG.error("Problems calling _closeLocalStores(): {}", e.getMessage(), e);
        }
        
        LOG.info("Local stores (databases) closed");
    }

    protected void setInitProblem(String prob) {
        _initProblem = prob;
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
        if (!_openLocalStores(true, true, true)) {
            return false;
        }
        _active.set(true);
        return true;
    }
    
    /**
     * Method called to open local stores if they exist, in read/write mode.
     */
    public boolean openIfExists()
    {
        if (!_openLocalStores(true, false, true)) {
            return false;
        }
        _active.set(true);
        return true;
    }

    /**
     * Method called to open local stores if they exist, and only open for reading.
     */
    public boolean openForReading(boolean log)
    {
        if (!_openLocalStores(log, false, false)) {
            return false;
        }
        _active.set(true);
        return true;
    }

    /**
     * 
     * @return True if opening succeeded; false if not; in latter case, activation
     *   will be considered failed
     */
    protected abstract boolean _openLocalStores(boolean log, boolean allowCreate, boolean writeAccess);

    protected abstract void _prepareToCloseLocalStores();
    
    protected abstract void _closeLocalStores();
    
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
    public StoredEntryConverter<K,E,?> getEntryConverter() { return _entryConverter; }
    
    @Override
    public StorableStore getEntryStore() { return _entryStore; }

    @Override
    public NodeStateStore<IpAndPort, ActiveNodeState> getNodeStore() { return _nodeStore; }

    @Override
    public NodeStateStore<IpAndPort, ActiveNodeState> getRemoteNodeStore() { return _remoteNodeStore; }
    
    @Override
    public LastAccessStore<K,E,LastAccessUpdateMethod> getLastAccessStore() { return null; }
}
