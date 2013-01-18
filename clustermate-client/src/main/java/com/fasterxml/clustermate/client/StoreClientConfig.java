package com.fasterxml.clustermate.client;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.client.operation.OperationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.client.call.CallConfig;
import com.fasterxml.storemate.shared.EntryKey;

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
