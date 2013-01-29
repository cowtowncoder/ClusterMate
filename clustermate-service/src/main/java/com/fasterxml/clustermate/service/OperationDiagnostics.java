package com.fasterxml.clustermate.service;

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
}
