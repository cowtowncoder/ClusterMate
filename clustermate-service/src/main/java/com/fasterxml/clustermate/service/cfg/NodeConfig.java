package com.fasterxml.clustermate.service.cfg;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.fasterxml.storemate.shared.IpAndPort;

public class NodeConfig
{
    @NotNull
    public IpAndPort ipAndPort;

    @Min(0)
    @Max(Integer.MAX_VALUE)
    public int keyRangeStart;

    // static config will not allow empty ranges (dynamic would, temporarily)
    @Min(1)
    @Max(Integer.MAX_VALUE)
    public int keyRangeLength;

    // ctor used by deserializer
    protected NodeConfig() { }

    // constructor for unit tests
    public NodeConfig(String endpoint, int keyStart, int keyLength)
    {
        this(new IpAndPort(endpoint), keyStart, keyLength);
    }

    public NodeConfig(IpAndPort endpoint, int keyStart, int keyLength)
    {
        ipAndPort = endpoint;
        keyRangeStart = keyStart;
        keyRangeLength = keyLength;
    }
}
