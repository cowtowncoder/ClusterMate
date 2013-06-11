package com.fasterxml.clustermate.dw;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;

/**
 * Metrics health check for use with DropWizard that checks health
 * of the cluster handling and state.
 */
public class HealthCheckForCluster extends HealthCheck
{
    protected final ClusterViewByServer _cluster;

    public HealthCheckForCluster(ServiceConfig config, ClusterViewByServer cluster)
    {
        _cluster = cluster;
    }

    @Override
    protected Result check() throws Exception
    {
        // first: see if we have 100% active coverage; if we do, great
        int activeCoverage = _cluster.getActiveCoveragePct();
        if (activeCoverage == 100) {
            return Result.healthy("Full Active coverage (100%)");
        }
        // If not, should we accept passive coverage as well
        int passiveCoverage = _cluster.getTotalCoveragePct();
        if (passiveCoverage == 100 && activeCoverage >= 50) {
            return Result.healthy("No full active coverage ("+activeCoverage+"%), but full passive");
        }
        return Result.unhealthy("Insufficient coverage: active="+activeCoverage+"%; passive "+passiveCoverage+"%");
    }
}
