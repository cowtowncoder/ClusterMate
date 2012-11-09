package com.fasterxml.clustermate.dw;

import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.yammer.metrics.core.HealthCheck;

public class HealthCheckForBDB extends HealthCheck
{
    protected final Stores<?,?> _bdb;
    
    public HealthCheckForBDB(ServiceConfig config, Stores<?,?> bdb)
    {
        super("BDB-JE");
        _bdb = bdb;
    }

    @Override
    protected Result check() throws Exception
    {
    	// TODO: check basics, like availability of BDB-JE
        if (_bdb.isActive()) {
            return Result.healthy();
        }
        String msg = _bdb.getInitProblem();
        if (msg != null) { // failed to start:
            return Result.unhealthy("BDB stores not active since initialization failed: "+msg);
        }
        return Result.unhealthy("BDB stores not active: no init problem set so most likely has been shut down");
    }
}
