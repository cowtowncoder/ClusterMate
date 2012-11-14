package com.fasterxml.clustermate.service.cfg;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Configuration settings for a single cluster node. Exact usage of
 * fields depends on {@link KeyRangeAllocationStrategy} used for
 * cluster handling, but all strategies at least use end point
 * definition.
 *<p> 
 * Note that two alternate representations are accepted: either "full"
 * one with multiple fields, represented by JSON Object; or "simple",
 * a String that only contains {@link #ipAndPort}. Latter is useful
 * for dynamic configurations.
 */
public class NodeConfig
{
    /**
     * Ip name (or number) and port that this node instance listens to.
     * Usually used in conjunction with node index (which is either
     * initialized externally or defined via {@link KeyRangeAllocationStrategy})
     * to match incoming configuration settings and stored node state
     * information.
     */
    @NotNull
    public IpAndPort ipAndPort;

    /**
     * Start offset of they key range this node covers.
     * Only used with
     * {@link KeyRangeAllocationStrategy#STATIC}; ignored with
     * other strategies.
     */
    @Min(0)
    @Max(Integer.MAX_VALUE)
    public int keyRangeStart = 0;

    /**
     * Length of they key range this node covers.
     * Only used with
     * {@link KeyRangeAllocationStrategy#STATIC}; ignored with
     * other strategies.
     */
    @Min(0)
    @Max(Integer.MAX_VALUE)
    public int keyRangeLength = 0;

    // Default ctor used by deserializer when binding from JSON Object
    protected NodeConfig() { }

    // Alternate internal ctor used when deserializing from simple String
    public NodeConfig(String str) throws IllegalArgumentException
    {
        try {
            ipAndPort = new IpAndPort(str);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid endpoint definition for constructing NodeConfig: \""
                    +str+"\"");
        }
    }

    // yet another constructor for tests, to use with non-static range settings
    public NodeConfig(IpAndPort endpoint) {
        this(endpoint, 0, 0);
    }

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
