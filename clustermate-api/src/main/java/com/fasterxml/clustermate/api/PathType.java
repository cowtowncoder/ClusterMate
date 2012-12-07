package com.fasterxml.clustermate.api;

/**
 * Enumeration for standard paths recognized by a service instance.
 */
public enum PathType
{
    // access to stored entries
    STORE_ENTRY,
    STORE_LIST,
    STORE_STATUS, // diagnostics interface

    // access to node status
    NODE_STATUS,
    
    // access to sync information
    SYNC_LIST,
    SYNC_PULL,
    
    ;
}
