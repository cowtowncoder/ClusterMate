package com.fasterxml.clustermate.service;

import java.io.IOException;
import java.util.List;

import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.service.cluster.ActiveNodeState;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Base class that defines operations for storing and retrieving persistent
 * Node state information.
 * Store is used for persisting enough state regarding
 * peer-to-peer operations to reduce amount of re-synchronization
 * needed when node instances are restarted; and typically should use the
 * strongest possible persistence and consistency guarantees that the
 * backing data store implementation can offer: rate of operations should
 * be low.
 */
public abstract class NodeStateStore
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    /*
    /**********************************************************************
    /* StartAndStoppable dummy implementation
    /**********************************************************************
     */

    @Override
    public void start() { }

    @Override
    public void prepareForStop() {
    }
    
    @Override
    public void stop() {
    }

    /*
    /**********************************************************************
    /* Public API: Content lookups
    /**********************************************************************
     */

    /**
     * Method that can be used to find specified entry, without updating
     * its last-accessed timestamp.
     */
    public abstract ActiveNodeState findEntry(IpAndPort key) throws IOException;

    /**
     * Method for simply reading all node entries store has; called usually
     * only during bootstrapping.
     */
    public abstract List<ActiveNodeState> readAll();

    /*
    /**********************************************************************
    /* Public API: Content modification
    /**********************************************************************
     */
    
    public abstract void upsertEntry(ActiveNodeState entry) throws IOException;

    public boolean deleteEntry(IpAndPort key) {
        return deleteEntry(key.toString());
    }

    public abstract boolean deleteEntry(String keyStr);
}

