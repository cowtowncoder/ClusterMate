package com.fasterxml.clustermate.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Sub-class of {@link ObjectMapper} that has custom handlers for
 * datatypes used by standard StoreMate servers and clients.
 */
@SuppressWarnings("serial")
public class ClusterMateObjectMapper extends ObjectMapper
{
    public ClusterMateObjectMapper()
    {
        // since these are JSON mappers, no point in numeric representation (false)
        registerModule(new ClusterMateTypesModule(false));
    }

    public ClusterMateObjectMapper(JsonFactory f)
    {
        super(f);
        // since these are JSON mappers, no point in numeric representation (false)
        registerModule(new ClusterMateTypesModule(false));
    }
}
