package com.fasterxml.clustermate.client;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.client.call.CallConfig;
import com.fasterxml.clustermate.client.operation.OperationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Base class for configuration types used with {@link StoreClient} implementations.
 *
 * @param <K> Type of keys used for retrieving entries from Store
 * @param <CONFIG> Recursive definition needed due to Java Generics oddities: sub-class of this base class
 */
public abstract class StoreClientConfig<
    K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>
>
{
    /**
     * This is just a simple "just-in-case" threshold to prevent message
     * flooding with retries; if things won't work with 5 retries (and initial
     * try, meaning 6 calls), we are probably hosed enough to give up
     * individual operations.
     */
    public final static int MAX_RETRIES_FOR_PUT = 5;

    /**
     * This is just a simple "just-in-case" threshold to prevent message
     * flooding with retries.
     * Assuming client can still retry operation, use slightly lower
     * value than for PUTs
     */
    public final static int MAX_RETRIES_FOR_GET = 3;

    /**
     * This is just a simple "just-in-case" threshold to prevent message
     * flooding with retries. Since DELETEs are bit more disposable,
     * let's use lower limit as well.
     */
    public final static int MAX_RETRIES_FOR_DELETE = 3;

    /**
     * Limit calls for cluster status to once every two seconds
     */
    public final static long MIN_DELAY_BETWEEN_STATUS_CALLS_MSECS = 2000L;

    /**
     * Add modest amount of delay between rounds of calls when we have failures,
     * just to reduce congestion during overloads
     */ 
    public final static long DELAY_BETWEEN_RETRY_ROUNDS_MSECS = 250L;

    // // // Core configuration settings

    protected final EntryKeyConverter<K> _keyConverter;

    protected final ObjectMapper _jsonMapper;

    protected final OperationConfig _operationConfig;

    protected final String[] _basePath;

    protected final RequestPathStrategy _pathStrategy;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    protected StoreClientConfig(EntryKeyConverter<K> keyConverter,
            String basePath[], RequestPathStrategy pathMapper,
            ObjectMapper jsonMapper,
            OperationConfig operConfig)
    {
        _keyConverter = keyConverter;
        _basePath = basePath;
        _pathStrategy = pathMapper;
        _jsonMapper = jsonMapper;
        _operationConfig = operConfig;
    }

    /**
     * Method to use to create a builder for creating alternate configurations,
     * using base settings from this instance.
     */
    public abstract <BUILDER extends StoreClientConfigBuilder<K, CONFIG, BUILDER>>
        BUILDER builder();
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */
    
    public EntryKeyConverter<K> getKeyConverter() {
        return _keyConverter;
    }

    /**
     * Accessor getting the base path (or "root" path) for services
     * to access. Relative paths are built from this path by appending.
     */
    public String[] getBasePath() {
        return _basePath;
    }

    public RequestPathStrategy getPathStrategy() {
        return _pathStrategy;
    }
    
    public ObjectMapper getJsonMapper() {
        return _jsonMapper;
    }
    
    /**
     * Accessor for per-call configuration settings.
     */
    public CallConfig getCallConfig() { return _operationConfig.getCallConfig(); }    
    
    public OperationConfig getOperationConfig() { return _operationConfig; }
}
