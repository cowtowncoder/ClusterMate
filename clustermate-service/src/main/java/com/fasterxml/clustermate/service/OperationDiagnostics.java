package com.fasterxml.clustermate.service;

import com.fasterxml.clustermate.service.util.TotalTime;
import com.fasterxml.storemate.store.Storable;

/**
 * Helper class used for requesting and returning per-operation statistics
 * so that caller can update metrics and diagnostic information
 */
public class OperationDiagnostics
{
    protected final long _nanoStart;
    
    protected Storable _entry;

    /**
     * Number of items included in response, for operations where this
     * makes sense.
     */
    protected int _itemCount;

    /**
     * Accumulated information on primary database read calls.
     */
    protected TotalTime _dbReads;

    /**
     * Accumulated information on primary database write calls.
     */
    protected TotalTime _dbWrites;

    protected TotalTime _lastAccessReads;

    protected TotalTime _lastAccessWrites;
    
    /*
    /**********************************************************************
    /* Construction, population
    /**********************************************************************
     */

    public OperationDiagnostics() {
        this(System.nanoTime());
    }

    public OperationDiagnostics(long nanoStart) {
        _nanoStart = nanoStart;
    }
    
    public OperationDiagnostics setEntry(Storable e) {
        _entry = e;
        return this;
    }

    public OperationDiagnostics setItemCount(int count) {
        _itemCount = count;
        return this;
    }

    public void addDbRead(long nanos) {
        _dbReads = TotalTime.createOrAdd(_dbReads, nanos);
    }

    public void addDbWrite(long nanos) {
        _dbWrites = TotalTime.createOrAdd(_dbWrites, nanos);
    }

    public void addLastAccessRead(long nanos) {
        _lastAccessReads = TotalTime.createOrAdd(_lastAccessReads, nanos);
    }

    public void addLastAccessWrite(long nanos) {
        _lastAccessWrites = TotalTime.createOrAdd(_lastAccessWrites, nanos);
    }
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */
    
    public Storable getEntry() { return _entry; }

    public int getItemCount() { return _itemCount; }
    
    /**
     * Accessor for number of nanoseconds spent since construction of this object
     */
    public long getNanosSpent() {
        return System.nanoTime() - _nanoStart;
    }

    public boolean hasDbReads() { return _dbReads != null; }
    public boolean hasDbWrites() { return _dbWrites != null; }
    public boolean hasLastAccessReads() { return _lastAccessReads != null; }
    public boolean hasLastAccessWrites() { return _lastAccessWrites != null; }

    public TotalTime getDbReads() { return _dbReads; }
    public TotalTime getDbWrites() { return _dbReads; }
    public TotalTime getLastAccessReads() { return _lastAccessReads; }
    public TotalTime getLastAccessWrites() { return _lastAccessWrites; }
}
