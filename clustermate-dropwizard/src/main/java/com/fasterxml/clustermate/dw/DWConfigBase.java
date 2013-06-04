package com.fasterxml.clustermate.dw;

import org.skife.config.TimeSpan;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.RequestLogConfiguration;
import com.yammer.dropwizard.util.Duration;

public abstract class DWConfigBase<
  SCONFIG extends ServiceConfig,
  THIS extends DWConfigBase<SCONFIG, THIS>
>
    extends Configuration
    implements Cloneable
{
    public DWConfigBase() {
        // Let's try forcing GZIP to be disabled: may not work wrt
        // data-binding (since that'll recreate objects but...)
        overrideGZIPEnabled(false);
    }

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

    @SuppressWarnings("unchecked")
    public THIS setSyncGracePeriod(TimeSpan t) {
        getServiceConfig().cfgSyncGracePeriod = t;
        return (THIS) this;
    }

    /**
     * Method called to suppress all request logging; usually during testing.
     */
    @SuppressWarnings("unchecked")
    public THIS disableRequestLog() {
        RequestLogConfiguration reqLog = getHttpConfiguration().getRequestLogConfiguration();
        reqLog.getConsoleConfiguration().setEnabled(false);
        reqLog.getFileConfiguration().setEnabled(false);
        reqLog.getSyslogConfiguration().setEnabled(false);
        return (THIS) this;
    }
}

