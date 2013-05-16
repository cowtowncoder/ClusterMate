package com.fasterxml.clustermate.service.metrics;

/**
 * Simple container for metrics from various store backends.
 */
public class StoreMetrics
{
    public BackendMetrics entries;

    public BackendMetrics entryIndex;

    public BackendMetrics lastAccessStore;
}
