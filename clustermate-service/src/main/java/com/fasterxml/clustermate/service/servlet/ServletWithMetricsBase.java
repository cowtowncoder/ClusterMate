package com.fasterxml.clustermate.service.servlet;

import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;

/**
 * Intermediate class we use to denote servlets that are to provide
 * metrics information.
 */
@SuppressWarnings("serial")
public abstract class ServletWithMetricsBase
    extends ServletBase
    implements AllOperationMetrics.Provider
{
    protected ServletWithMetricsBase(ClusterViewByServer clusterView,
            String servletPathBase)
    {
        super(clusterView, servletPathBase);
    }
    
    @Override
    public abstract void fillOperationMetrics(AllOperationMetrics metrics);
}