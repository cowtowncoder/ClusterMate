package com.fasterxml.clustermate.service.cfg;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public class ClusterConfig
{
    /**
     * Type of key range allocation strategy in use; determines how other settings
     * are used.
     */
    @NotNull
    public KeyRangeAllocationStrategy type;

    /**
     * Size of the key space, used for all key range allocation strategies.
     * Key space is usually divided in 
     *<p>
     * For {@link KeyRangeAllocationStrategy#DYNAMIC_WITH_APPEND} it must be
     * power 
     */
    @Min(1)
    @Max(Integer.MAX_VALUE)
    public int clusterKeyspaceSize;

    /**
     * Number of copies that should be stored for each entry: typically either
     * 3 (higher redundancy) or 2 (lower), although other values are legal
     * too (1 means there is no redundancy).
     * Value of 0 is allowed since this is optional for certain configurations;
     * and -1 may be used as marker for special value ("maximum copies", usually)
     */
    @Min(-1)
    @Max(Integer.MAX_VALUE)
    public int numberOfCopies;

    /**
     * Whether new nodes can register dynamically, without being included in
     * configuration settings. Usually this is what is wanted, but it may
     * be disabled for static set ups if that is considered a stability
     * or security risk.
     */
    public boolean allowDynamicJoining = true;
 
    /**
     * Smallest number of nodes that cluster may have; used for some
     * allocation strategies to know meaning of "doubling up". Typical
     * number used is 2, 3 or 4.
     *<p>
     * Note: not used with {@link KeyRangeAllocationStrategy#STATIC}
     * or {@link KeyRangeAllocationStrategy#SIMPLE_LINEAR} strategies.
     */
    public int baseNodeCount = 1;
    
    // // Static configuration of cluster nodes; either complete or partial
    // // (latter for bootstrapping)

    // not mandatory, but if defined, must be valid
    @Valid
    // also: if defined, must have at least 1 entry
    @Size(min=1, max=Integer.MAX_VALUE)
    public NodeConfig[] clusterNodes; // = new ArrayList<NodeConfig>();
}
