package com.fasterxml.clustermate.dw;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.util.Duration;

public abstract class DWConfigBase<
  SCONFIG extends ServiceConfig,
  THIS extends DWConfigBase<SCONFIG, THIS>
>
    extends Configuration
    implements Cloneable
{
    public DWConfigBase() { }

    /*
    /**********************************************************************
    /* Abstract accessors
    /**********************************************************************
     */

    public abstract SCONFIG getServiceConfig();
    
    /*
    /**********************************************************************
    /* Copying
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    public THIS clone() {
        try {
            return (THIS) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone "+getClass().getName()+", WTH?");
        }
    }
    
    /*
    /**********************************************************************
    /* Additional mutators; needed to work around DW strict access
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    public THIS overrideHttpPort(int p) {
        getHttpConfiguration().setPort(p);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideAdminPort(int p) {
        getHttpConfiguration().setAdminPort(p);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideGZIPEnabled(boolean state) {
        getHttpConfiguration().getGzipConfiguration().setEnabled(state);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS setShutdownGracePeriod(Duration d) {
        getHttpConfiguration().setShutdownGracePeriod(d);
        return (THIS) this;
    }
}
