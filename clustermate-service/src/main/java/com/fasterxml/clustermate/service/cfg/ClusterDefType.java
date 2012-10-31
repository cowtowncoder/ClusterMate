package com.fasterxml.clustermate.service.cfg;

/**
 * Enumeration used for distinguishing different configurations
 * for cluster information: currently just file-based "static",
 * and bootstrap-node based "dynamic" variant.
 */
public enum ClusterDefType
{
    /**
     * Dynamic configuration means that all we get is a small set of
     * end points which will provide actual information; these are
     * usually regular nodes (or some subset thereof), but could
     * be stand-alone manager nodes.
     */
    DYNAMIC,

    /**
     * Static configuration means that all configuration comes from the
     * main configuration file, and includes all cluster nodes along
     * with key range information. This is useful for testing, and simple
     * deployments where cluster setup is unlikely to change, or can be
     * changed with full restart (and related outage)
     */
    STATIC
    ;
}
