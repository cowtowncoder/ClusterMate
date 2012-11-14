package com.fasterxml.clustermate.service.cfg;

/**
 * Enumeration used for distinguishing different configurations
 * for cluster information, mainly allocation of ranges of key space
 * for nodes.
 */
public enum KeyRangeAllocationStrategy
{
    /**
     * Fully static configuration means that all configuration comes from the
     * main configuration file, and includes all cluster nodes along
     * with key range information. This is useful for testing, and simple
     * deployments where cluster setup is unlikely to change, or can be
     * changed with full (but usually incremental) restart of nodes of
     * the cluster.
     *<p>
     * No additional node discovery is made, although additional nodes may
     * join cluster if this is enabled.
     */
    STATIC,
    
    /**
     * Configuration setting in which keyspace is allocated evenly to each
     * node, starting from beginning, and using overlapping setting depending
     * on desired number of copies to keep.
     *<p>
     * When adding nodes in cluster, care must be taken to either add nodes
     * one by one (and waiting until cluster is fully settled), deploying
     * with new configuration between each addition, or doubling up by
     * adding nodes in interleaved fashion. Either way, the goal is to avoid
     * temporary gaps in coverage; typically addition does reduce number of
     * available copies by one for affected regions.
     * If this is not acceptable, other allocation strategies should be used:
     * either {@link #STATIC} (to manually define ranges with extra redundancy)
     * or {@link #DYNAMIC_WITH_APPEND} to let system organize intermediate
     * overlaps appropriately.
     *<p>
     * No additional node discovery is made, although additional nodes may
     * join cluster if this is enabled.
     */
    SIMPLE_LINEAR,

    /**
     * Configuration setting in which cluster size can be increased by simply
     * adding new nodes at the end of the node list.
     * Key ranges are dynamically calculated (initially) as well as adjusted when
     * cluster grows.
     *<p>
     * When adding nodes, append must be done at the end, and one at a time. New
     * nodes should only be appended once new node has "caught up" with the node
     * it took part of key range from.
     *<p>
     * No additional node discovery is made, although additional nodes may
     * join cluster if this is enabled.
     */
    DYNAMIC_WITH_APPEND,

    ;
}
