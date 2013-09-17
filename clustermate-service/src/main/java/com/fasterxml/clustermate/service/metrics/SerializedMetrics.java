package com.fasterxml.clustermate.service.metrics;

public final class SerializedMetrics
{
    public final long created;
    public final byte[] serialized;

    public SerializedMetrics(long cr, byte[] data) {
        created = cr;
        serialized = data;
    }
}