package com.fasterxml.clustermate.api;

/**
 * Enumeration for standard paths recognized by a service instance.
 */
public enum PathType
{
    // access to stored entries
    STORE_ENTRY, // single-entry CRUD
    STORE_LIST, // multi-entry listings
    STORE_STATUS, // diagnostics interface

    // access to node status: GET for status, PUT for update, POST for hello/bye
    NODE_STATUS,

    // access to sync information
    SYNC_LIST, // request for change list (ids)
    SYNC_PULL, // request for specific (changed/new) entries
    
    ;
}
