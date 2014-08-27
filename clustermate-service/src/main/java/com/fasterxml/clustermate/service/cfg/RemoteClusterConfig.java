package com.fasterxml.clustermate.service.cfg;

import javax.validation.Valid;
import javax.validation.constraints.*;

import com.fasterxml.storemate.shared.IpAndPort;

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
    /**
     * Setting that determines level of redundancy to use for pulling content
     * from remote cluster(s): between values of 1 and local custer size
     * (that is, {@link ClusterConfig#numberOfCopies}). Special value
     * of -1 may be used to mean "use same value as local 'numberOfCopies'".
     * Choice here is between reliable replication (higher value) and minimal
     * transfer rates (lowest, that is, 1).
     * Value itself is used similar to how local node allocation goes, but just
     * for calculating shared key range portion to request from matching remote
     * node.
     */
    @Min(-1)
    public int copiesToFetch;
    
    // // Static configuration of cluster nodes; either complete or partial
    // // since this is only used for boostrapping and actual remote cluster
    // // topology is dynamically requested.

    // not mandatory, but if defined, must be valid
    // also: if defined, must have at least 1 entry
    @Valid
    @Size(min=1, max=Integer.MAX_VALUE)
    public IpAndPort[] nodes;
}
