package com.fasterxml.clustermate.service.cfg;

import javax.validation.Valid;
import javax.validation.constraints.*;

/**
 * Configuration settings for one of optional "remote" clusters; remote
 * clusters are clusters with which local cluster exchanges data, and
 * the typical use case is to allow Data Center replication for
 * Disaster Recovery (DR) or failover, or for providing read-only replicas.
 *<p>
 * Note that key ranges of all clusters must match; otherwise content
 * exchange will not work. Because of this, configuration here is much
 * more compact than that of {@link ClusterConfig}; in fact, only one
 * or more seed nodes are needed.
 */
public class RemoteClusterConfig
{
    // // Static configuration of cluster nodes; either complete or partial
    // // since this is only used for boostrapping and actual remote cluster
    // // topology is dynamically requested.

    // not mandatory, but if defined, must be valid
    @Valid
    // also: if defined, must have at least 1 entry
    @Size(min=1, max=Integer.MAX_VALUE)
    public NodeConfig[] nodes;
}
