package com.fasterxml.clustermate.dw;

import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.yammer.metrics.core.HealthCheck;

public class HealthCheckForStore extends HealthCheck
{
    protected final Stores<?,?> _stores;
    
    public HealthCheckForStore(ServiceConfig config, Stores<?,?> stores)
    {
        super("StorableStore");
        _stores = stores;
    }

    @Override
    protected Result check() throws Exception
    {
        // TODO: check basics, like availability of backend data store
        if (_stores.isActive()) {
            return Result.healthy();
        }
        String msg = _stores.getInitProblem();
        if (msg != null) { // failed to start:
            return Result.unhealthy("StorableStore not active since initialization failed: "+msg);
        }
        return Result.unhealthy("StorableStore not active: no init problem set so most likely has been shut down");
    }
}
