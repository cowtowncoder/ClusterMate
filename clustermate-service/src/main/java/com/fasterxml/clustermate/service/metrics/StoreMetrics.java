package com.fasterxml.clustermate.service.metrics;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Simple container for metrics from various store backends.
 */
@JsonPropertyOrder({ "entries", "entryIndex", "lastAccessStore" })
public class StoreMetrics
{
    public BackendMetrics entries;

    public BackendMetrics entryIndex;

    public BackendMetrics lastAccessStore;
}
