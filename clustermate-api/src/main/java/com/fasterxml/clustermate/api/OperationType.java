package com.fasterxml.clustermate.api;

/**
 * Enumeration of typical request types, based on HTTP method definitions.
 */
public enum OperationType {
    GET,
    HEAD,
    PUT,
    POST,
    DELETE,

    // and then less common bones
    CONNECT,
    OPTIONS,
    PATCH,
    TRACE,
    
    // Plus fallback for "other"...
    CUSTOM
    ;
}
